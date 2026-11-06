using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.AllocationProfile;

internal static partial class Scenarios
{
    internal enum Location
    {
        /// <summary>All keys in the active memtable.</summary>
        Memtable,
        /// <summary>One compacted SST, all data blocks in a 64 MiB block cache.</summary>
        Cached,
        /// <summary>One compacted SST (~13 MB of blocks), 1 MiB block cache: nearly every Get reads a block from the page cache.</summary>
        Uncached,
    }

    internal const int ReadKeys = 100_000;

    /// <summary>A DB with <see cref="ReadKeys"/> keys at <paramref name="location"/>, plus the keys in random order.</summary>
    internal static (RocksDatabase Db, byte[][] Keys) ReadDb(ScenarioContext ctx, Location location, int valueSize = ScenarioContext.ValueSize,
        Action<DbOptions, ColumnFamilyOptions>? configure = null)
    {
        ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions(location == Location.Uncached ? 1L << 20 : 64L << 20);
        o.DisableAutoCompactions = true;
        if (location == Location.Memtable) o.WriteBufferSize = 256UL << 20;
        DbOptions d = ScenarioContext.DefaultDbOptions();
        configure?.Invoke(d, o);
        RocksDatabase db = ctx.OpenDb("db", d, o);
        byte[][] keys = ScenarioContext.Keys(ReadKeys);
        ScenarioContext.Fill(db, keys, ScenarioContext.Value(valueSize), flush: location != Location.Memtable, compact: location != Location.Memtable);
        byte[][] shuffled = ScenarioContext.Shuffled(keys);
        if (location == Location.Cached) ScenarioContext.WarmCache(db, shuffled);
        return (db, shuffled);
    }

    private static string Where(Location l) => l switch
    {
        Location.Memtable => "memtable",
        Location.Cached => "sst-cached",
        _ => "sst-uncached",
    };

    private static void AddReads(List<Scenario> list)
    {
        foreach (Location loc in (Location[])[Location.Memtable, Location.Cached, Location.Uncached])
        {
            string w = Where(loc);
            Location l = loc;
            list.Add(S($"get.bytes.{w}", "get", PathKind.Hot, $"byte[]? Get(key), {w}", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, l);
                var c = new Cursor(keys.Length);
                long sum = 0;
                return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()])!.Length; }));
            }, notes: "Allocates the result array by design (100 B value -> 128 B byte[])."));
            list.Add(S($"get.span.{w}", "get", PathKind.Hot, $"TryGet(key, Span<byte>, out len), {w}", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, l);
                byte[] buf = new byte[256];
                var c = new Cursor(keys.Length);
                return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
            }));
            list.Add(S($"get.async.{w}", "get", PathKind.Hot, $"await TryGetAsync(key, IBufferWriter), {w}", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, l);
                var writer = new ArrayBufferWriter<byte>(256);
                var c = new Cursor(keys.Length);
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        writer.ResetWrittenCount();
                        await db.TryGetAsync(keys[c.Next()], writer);
                    }
                }));
            }));
        }
        list.Add(S("get.bufferwriter.sst-cached", "get", PathKind.Hot, "TryGet(key, IBufferWriter) into a reused ArrayBufferWriter", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var writer = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { writer.ResetWrittenCount(); db.TryGet(keys[c.Next()], writer); } }));
        }));
        list.Add(S("get.callback.sst-cached", "get", PathKind.Hot, "Get<TState,TResult>(key, state, static span callback)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()], 0, static (v, _) => v.Length); }));
        }));
        list.Add(S("get.pooled.sst-cached", "get", PathKind.Hot, "using PooledValue? = GetPooled(key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { using PooledValue? v = db.GetPooled(keys[c.Next()]); sum += v!.Length; } }));
        }));
        list.Add(S("get.pooled-async.sst-cached", "get", PathKind.Hot, "using PooledValue? = await GetPooledAsync(key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) { using PooledValue? v = await db.GetPooledAsync(keys[c.Next()]); sum += v!.Length; }
            }));
        }));
        list.Add(S("get.bytes-async.sst-cached", "get", PathKind.Hot, "byte[]? await GetAsync(key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) sum += (await db.GetAsync(keys[c.Next()]))!.Length;
            }));
        }));
        list.Add(S("get.miss.sst-cached", "get", PathKind.Hot, "TryGet(Span) of a missing key (bloom filter)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            byte[][] keys = ScenarioContext.Shuffled(ScenarioContext.Keys(ReadKeys, missing: true));
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("containskey.sst-cached", "get", PathKind.Hot, "ContainsKey(key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.ContainsKey(keys[c.Next()]); }));
        }));
        list.Add(S("keymayexist.sst-cached", "get", PathKind.Hot, "KeyMayExist(key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.KeyMayExist(keys[c.Next()]); }));
        }));
        list.Add(S("get.snapshot.sst-cached", "get", PathKind.Hot, "TryGet(ReadOptions{Snapshot} reused, key, Span)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            Snapshot snap = ctx.Own(db.GetSnapshot());
            var ro = new ReadOptions { Snapshot = snap };
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(ro, keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("get.new-readoptions.sst-cached", "get", PathKind.Hot, "TryGet(new ReadOptions{FillCache=false}, key, Span) per op", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(new ReadOptions { FillCache = false }, keys[c.Next()], buf, out _); }));
        }, notes: "Caller pattern: the ReadOptions object itself is the allocation; reuse one."));
        list.Add(S("get.cf.sst-cached", "get", PathKind.Hot, "ColumnFamily.TryGet(key, Span) on a second column family", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            ColumnFamily cf = db.CreateColumnFamily("other", ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(ReadKeys);
            ScenarioContext.Fill(cf, keys, ScenarioContext.Value(100), flush: true);
            byte[] buf = new byte[256];
            byte[][] sk = ScenarioContext.Shuffled(keys);
            foreach (byte[] k in sk) cf.TryGet(k, buf, out _);
            var c = new Cursor(sk.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) cf.TryGet(sk[c.Next()], buf, out _); }));
        }));
        list.Add(S("get.span.4threads.sst-cached", "get", PathKind.Hot, "TryGet(Span) from 4 threads, per Get", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            return Done(Runner.Concurrent(4, (w, n) =>
            {
                Span<byte> buf = stackalloc byte[256];
                int off = w * 7919;
                for (int i = 0; i < n; i++) db.TryGet(keys[(off + i) % keys.Length], buf, out _);
                return default;
            }));
        }, chunk: 40_000));
        list.Add(S("get.statistics.sst-cached", "get", PathKind.Hot, "TryGet(Span) with DbOptions.Statistics enabled", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached, configure: static (d, _) => d.Statistics = new Statistics());
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _); }));
        }));
        list.Add(S("get.perfcontext.sst-cached", "get", PathKind.Hot, "TryGet(Span) with PerfContext.Level = EnableTimeExceptForMutex", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                PerfContext.Level = PerfLevel.EnableTimeExceptForMutex;
                for (int i = 0; i < n; i++) db.TryGet(keys[c.Next()], buf, out _);
                PerfContext.Current.Reset();
                PerfContext.Level = PerfLevel.Disable;
            }));
        }));
        list.Add(S("get.merge.uint64add", "get", PathKind.Hot, "Get<TState,TResult> of a key with 4 unmerged UInt64Add operands (memtable)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = MergeOperator.UInt64Add;
            o.WriteBufferSize = 64UL << 20;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(20_000);
            byte[] one = new byte[8];
            one[0] = 1;
            foreach (byte[] k in keys) for (int j = 0; j < 4; j++) db.Merge(k, one);
            var c = new Cursor(keys.Length);
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()], 0, static (v, _) => BinaryPrimitives.ReadUInt64LittleEndian(v)); }));
        }));
        list.Add(S("get.merge.user", "get", PathKind.Hot, "Get<TState,TResult> of a key with 4 unmerged user-operator operands (memtable)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = new Int64MaxOperator();
            o.WriteBufferSize = 64UL << 20;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(20_000);
            byte[] v8 = new byte[8];
            foreach (byte[] k in keys) for (int j = 0; j < 4; j++) { v8[0] = (byte)j; db.Merge(k, v8); }
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()], 0L, static (v, _) => BinaryPrimitives.ReadInt64LittleEndian(v)); }));
        }));
        list.Add(S("get.udt.sst-cached", "get", PathKind.Hot, "TryGet(ReadOptions{Timestamp}, key, value writer, timestamp writer)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.Comparator = Comparator.BytewiseWithU64Ts;
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(ReadKeys);
            byte[] ts = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(ts, 10);
            byte[] val = ScenarioContext.Value(100);
            foreach (byte[] k in keys) db.Put(k, ts, val);
            db.Flush();
            byte[] readTs = new byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(readTs, 20);
            var ro = new ReadOptions { Timestamp = readTs };
            var vw = new ArrayBufferWriter<byte>(256);
            var tw = new ArrayBufferWriter<byte>(16);
            byte[][] sk = ScenarioContext.Shuffled(keys);
            var c = new Cursor(sk.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    vw.ResetWrittenCount();
                    tw.ResetWrittenCount();
                    db.TryGet(ro, sk[c.Next()], vw, tw);
                }
            }));
        }));
        list.Add(S("get.blob", "get", PathKind.Hot, "TryGet(key, Span) of a 4 KiB value in a blob file (no blob cache)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.EnableBlobFiles = true;
            o.MinBlobSize = 512;
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(5_000);
            ScenarioContext.Fill(db, keys, ScenarioContext.Value(4096), flush: true, compact: false);
            byte[] buf = new byte[8192];
            byte[][] sk = ScenarioContext.Shuffled(keys);
            var c = new Cursor(sk.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGet(sk[c.Next()], buf, out _); }));
        }));

        // MultiGet.
        foreach (int size in (int[])[16, 128])
        {
            int sz = size;
            list.Add(S($"multiget.batch{sz}.sst-cached", "multiget", PathKind.Hot, $"reused MultiGetBatch: Clear + {sz} Add + MultiGet (per batch)", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
                var batch = ctx.Own(new MultiGetBatch(sz));
                var c = new Cursor(keys.Length);
                return Done(Runner.Sync(n =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        batch.Clear();
                        for (int j = 0; j < sz; j++) batch.Add(keys[c.Next()]);
                        db.MultiGet(batch);
                    }
                }));
            }, chunk: 50));
            list.Add(S($"multiget.batch{sz}-async.sst-uncached", "multiget", PathKind.Hot, $"reused MultiGetBatch: {sz} keys, await MultiGetAsync, page-cache reads", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Uncached);
                var batch = ctx.Own(new MultiGetBatch(sz));
                var c = new Cursor(keys.Length);
                return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        batch.Clear();
                        for (int j = 0; j < sz; j++) batch.Add(keys[c.Next()]);
                        await db.MultiGetAsync(batch);
                    }
                }));
            }, chunk: 20));
        }
        list.Add(S("multiget.legacy16.sst-cached", "multiget", PathKind.Hot, "byte[]?[] MultiGet(IReadOnlyList<byte[]>) of 16 keys (per batch)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var groups = new byte[keys.Length / 16][][];
            for (int g = 0; g < groups.Length; g++) groups[g] = keys.AsSpan(g * 16, 16).ToArray();
            var c = new Cursor(groups.Length);
            long found = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) found += db.MultiGet(groups[c.Next()]).Length; }));
        }, chunk: 50, notes: "Allocates the result array and one byte[] per found value by design."));

        // Wide columns.
        list.Add(S("getentity.new", "entity", PathKind.Hot, "WideColumns? GetEntity(key) (new result per call)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetEntity(keys[c.Next()])!.Count; }));
        }));
        list.Add(S("getentity.reuse", "entity", PathKind.Hot, "TryGetEntity(key, reused WideColumns)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            var columns = new WideColumns();
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.TryGetEntity(keys[c.Next()], columns); }));
        }));
        list.Add(S("getentity.async", "entity", PathKind.Hot, "await TryGetEntityAsync(null, key, reused WideColumns)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            var columns = new WideColumns();
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.DefaultColumnFamily.TryGetEntityAsync(null, keys[c.Next()], columns);
            }));
        }));
        list.Add(S("getentity.lazy", "entity", PathKind.Hot, "using LazyWideColumns? GetEntityLazy(null, key)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { using LazyWideColumns? l = db.DefaultColumnFamily.GetEntityLazy(null, keys[c.Next()]); sum += l!.Count; } }));
        }));
        list.Add(S("getentity.lazy-reuse", "entity", PathKind.Hot, "TryGetEntityLazy(null, key, reused LazyWideColumns)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            LazyWideColumns lazy = ctx.Own(new LazyWideColumns());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.DefaultColumnFamily.TryGetEntityLazy(null, keys[c.Next()], lazy); sum += lazy.Count; } lazy.Reset(); }));
        }));
        list.Add(S("getmergeoperands", "get", PathKind.Hot, "TryGetMergeOperands(key, options, reused MergeOperandsResult) of 4 operands (memtable)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = MergeOperator.UInt64Add;
            o.WriteBufferSize = 64UL << 20;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(20_000);
            byte[] one = new byte[8];
            one[0] = 1;
            foreach (byte[] k in keys) for (int j = 0; j < 4; j++) db.Merge(k, one);
            var mo = new GetMergeOperandsOptions { ExpectedMaxNumberOfOperands = 16 };
            var result = new MergeOperandsResult();
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.DefaultColumnFamily.TryGetMergeOperands(keys[c.Next()], mo, result); }));
        }));
        list.Add(S("multigetentity.batch16", "entity", PathKind.Hot, "reused MultiGetBatch: 16 keys, MultiGetEntity + TryGetEntity(reused WideColumns)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            var batch = ctx.Own(new MultiGetBatch(16));
            var columns = new WideColumns();
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    batch.Clear();
                    for (int j = 0; j < 16; j++) batch.Add(keys[c.Next()]);
                    db.MultiGetEntity(batch);
                    for (int j = 0; j < 16; j++) batch.TryGetEntity(j, columns);
                }
            }));
        }, chunk: 50));
        list.Add(S("wbwi.get-from-batch-and-db", "batch", PathKind.Hot, "TryGetFromBatchAndDB(db, key, writer) with 100 keys in the batch", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var wbwi = ctx.Own(new WriteBatchWithIndex());
            byte[] v = ScenarioContext.Value(100, 9);
            for (int i = 0; i < 100; i++) wbwi.Put(keys[i * 7], v);
            var writer = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { writer.ResetWrittenCount(); wbwi.TryGetFromBatchAndDB(db, keys[c.Next()], writer); } }));
        }));
    }

    /// <summary>A DB of 50k entities (3 columns) in one compacted, cached SST.</summary>
    internal static (RocksDatabase Db, byte[][] Keys) EntityDb(ScenarioContext ctx)
    {
        ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
        o.DisableAutoCompactions = true;
        RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
        byte[][] keys = ScenarioContext.Keys(50_000);
        WideColumn[] cols = Columns();
        foreach (byte[] k in keys) db.PutEntity(k, cols);
        db.Flush();
        db.CompactRange();
        byte[][] sk = ScenarioContext.Shuffled(keys);
        var wc = new WideColumns();
        foreach (byte[] k in sk) db.TryGetEntity(k, wc);
        return (db, sk);
    }
}
