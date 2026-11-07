using System.Runtime.CompilerServices;

namespace RocksDb.Managed.AllocationProfile;

internal static partial class Scenarios
{
    private static void AddIterators(List<Scenario> list)
    {
        list.Add(S("iter.create-dispose", "iterator", PathKind.Warm, "NewIterator() + Dispose (no positioning)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.NewIterator().Dispose(); }));
        }));
        list.Add(S("iter.create-seek-next10", "iterator", PathKind.Hot, "NewIterator + Seek + 10 Next (Key/Value spans) + Dispose", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Iterator it = db.NewIterator();
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length + it.Value.Length;
                }
            }));
        }));
        foreach (Location loc in (Location[])[Location.Memtable, Location.Cached, Location.Uncached])
        {
            Location l = loc;
            string w = Where(loc);
            list.Add(S($"iter.seek-next10.{w}", "iterator", PathKind.Hot, $"reused Iterator: Seek + 10 Next, {w}", ctx =>
            {
                (RocksDatabase db, byte[][] keys) = ReadDb(ctx, l);
                Iterator it = ctx.Own(db.NewIterator());
                var c = new Cursor(keys.Length);
                long sum = 0;
                return Done(Runner.Sync(n =>
                {
                    for (int i = 0; i < n; i++)
                    {
                        it.Seek(keys[c.Next()]);
                        for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length + it.Value.Length;
                    }
                }));
            }));
        }
        list.Add(S("iter.seek-next10-async.sst-uncached", "iterator", PathKind.Hot, "reused Iterator: await SeekAsync + 10 await NextAsync", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Uncached);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    await it.SeekAsync(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++)
                    {
                        sum += it.Key.Length;
                        await it.NextAsync();
                    }
                }
            }));
        }));
        list.Add(S("iter.create-async.sst-cached", "iterator", PathKind.Hot, "await NewIteratorAsync + SeekAsync + 10 NextAsync + Dispose", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Iterator it = await db.NewIteratorAsync(null);
                    await it.SeekAsync(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++)
                    {
                        sum += it.Key.Length;
                        await it.NextAsync();
                    }
                }
            }));
        }));
        list.Add(S("iter.reverse.sst-cached", "iterator", PathKind.Hot, "reused Iterator: SeekForPrev + 10 Prev", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.SeekForPrev(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Prev()) sum += it.Value.Length;
                }
            }));
        }));
        list.Add(S("iter.next.sst-cached", "iterator", PathKind.Hot, "reused Iterator: one Next (SeekToFirst when exhausted)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            Iterator it = ctx.Own(db.NewIterator());
            it.SeekToFirst();
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    if (!it.Valid) it.SeekToFirst();
                    sum += it.Value.Length;
                    it.Next();
                }
            }));
        }, chunk: 10_000));
        list.Add(S("iter.bounds-per-scan.sst-cached", "iterator", PathKind.Hot, "new ReadOptions{Lower/UpperBound} + NewIterator + SeekToFirst + scan 10 + Dispose", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            byte[][] sorted = ScenarioContext.Keys(ReadKeys);
            var c = new Cursor(sorted.Length - 10);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    int k = c.Next();
                    using Iterator it = db.NewIterator(new ReadOptions { IterateLowerBound = sorted[k], IterateUpperBound = sorted[k + 10] });
                    for (it.SeekToFirst(); it.Valid; it.Next()) sum += it.Key.Length;
                }
            }));
        }, notes: "Typical caller pattern; ReadOptions + boxed ReadOnlyMemory? bounds are caller allocations."));
        list.Add(S("iter.setbounds-per-scan.sst-cached", "iterator", PathKind.Hot, "reused Iterator: SetBounds(lower, upper) + SeekToFirst + scan 10", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            byte[][] sorted = ScenarioContext.Keys(ReadKeys);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(sorted.Length - 10);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    int k = c.Next();
                    it.SetBounds(sorted[k], sorted[k + 10]);
                    for (it.SeekToFirst(); it.Valid; it.Next()) sum += it.Key.Length;
                }
            }));
        }));
        list.Add(S("iter.refresh-per-scan.sst-cached", "iterator", PathKind.Hot, "reused Iterator: Refresh + Seek + 10 Next (sees new writes)", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Refresh();
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
                }
            }));
        }));
        list.Add(S("iter.range-foreach.sst-cached", "iterator", PathKind.Hot, "reused Iterator: foreach over Range(start, end) of 10 keys", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            byte[][] sorted = ScenarioContext.Keys(ReadKeys);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(sorted.Length - 10);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    int k = c.Next();
                    foreach (KeyValueRef kv in it.Range(sorted[k], sorted[k + 10])) sum += kv.Value.Length;
                }
            }));
        }));
        list.Add(S("iter.scanasync.sst-cached", "iterator", PathKind.Hot, "reused Iterator: await foreach ScanAsync(start, end) of 10 keys", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            byte[][] sorted = ScenarioContext.Keys(ReadKeys);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(sorted.Length - 10);
            long sum = 0;
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    int k = c.Next();
                    await foreach (KeyValue kv in it.ScanAsync(sorted[k], sorted[k + 10])) sum += kv.Value.Length;
                }
            }));
        }));
        list.Add(S("iter.prefix.sst-cached", "iterator", PathKind.Hot, "reused Iterator{PrefixSameAsStart}: Seek(prefix) + scan the prefix (10 keys)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.PrefixExtractor = SliceTransform.CreateFixedPrefix(15);   // "k" + 14 digits: groups of 10 keys
            o.DisableAutoCompactions = true;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(ReadKeys);
            ScenarioContext.Fill(db, keys, ScenarioContext.Value(100), flush: true, compact: true);
            Iterator it = ctx.Own(db.NewIterator(new ReadOptions { PrefixSameAsStart = true }));
            byte[][] prefixes = new byte[ReadKeys / 10][];
            for (int p = 0; p < prefixes.Length; p++) prefixes[p] = keys[p * 10].AsSpan(0, 15).ToArray();
            prefixes = ScenarioContext.Shuffled(prefixes);
            var c = new Cursor(prefixes.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                    for (it.Seek(prefixes[c.Next()]); it.Valid; it.Next()) sum += it.Value.Length;
            }));
        }));
        list.Add(S("iter.tailing", "iterator", PathKind.Hot, "reused tailing Iterator: Put new key + Seek(it) (memtable + SST)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(400_000);
            byte[] v = ScenarioContext.Value(100);
            Iterator it = ctx.Own(db.NewIterator(new ReadOptions { Tailing = true }));
            var c = new Cursor(keys.Length);
            var wo = new WriteOptions { DisableWal = true };
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    byte[] k = keys[c.Next()];
                    db.Put(wo, k, v);
                    it.Seek(k);
                }
            }));
        }, notes: "Includes the Put (see put.nowal for its share)."));
        list.Add(S("iter.snapshot.sst-cached", "iterator", PathKind.Hot, "GetSnapshot + NewIterator(ReadOptions{Snapshot}) + Seek + 10 Next + Dispose both", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var ro = new ReadOptions();
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Snapshot s = db.GetSnapshot();
                    ro.Snapshot = s;
                    using Iterator it = db.NewIterator(ro);
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
                }
            }));
        }));
        list.Add(S("iter.entity-columns", "iterator", PathKind.Hot, "reused Iterator over entities: Seek + 10 x (Columns.Count) Next", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = EntityDb(ctx);
            Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Columns.Count;
                }
            }));
        }));
        list.Add(S("iter.wbwi-base-delta", "iterator", PathKind.Warm, "wbwi.NewIteratorWithBase(db.NewIterator()) + Seek + 10 Next + Dispose", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            var wbwi = ctx.Own(new WriteBatchWithIndex());
            byte[] v = ScenarioContext.Value(100, 9);
            for (int i = 0; i < 100; i++) wbwi.Put(keys[i * 7], v);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using BaseDeltaIterator it = wbwi.NewIteratorWithBase(db.NewIterator());
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
                }
            }));
        }));
    }
}
