using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.AllocationProfile;

internal static partial class Scenarios
{
    private static void AddMisc(List<Scenario> list)
    {
        list.Add(S("snapshot.get-release", "snapshot", PathKind.Warm, "GetSnapshot + Dispose", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Memtable);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.GetSnapshot().Dispose(); }));
        }));
        list.Add(S("cf.get-by-name", "columnfamily", PathKind.Warm, "GetColumnFamily(name) + TryGetColumnFamily(name)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            db.CreateColumnFamily("users", ScenarioContext.DefaultCfOptions());
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.GetColumnFamily("users"); db.TryGetColumnFamily("users", out _); } }));
        }));
        list.Add(S("cf.list-handles", "columnfamily", PathKind.Warm, "db.ColumnFamilies (3 CFs)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            db.CreateColumnFamily("a", ScenarioContext.DefaultCfOptions());
            db.CreateColumnFamily("b", ScenarioContext.DefaultCfOptions());
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.ColumnFamilies.Count; }));
        }));
        list.Add(S("cf.create-drop", "columnfamily", PathKind.Cold, "CreateColumnFamily + DropColumnFamily", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            int id = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.DropColumnFamily(db.CreateColumnFamily("cf" + id++, o)); }));
        }, chunk: 5));
        list.Add(S("property.int", "property", PathKind.Warm, "TryGetIntProperty(rocksdb.estimate-num-keys)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.TryGetIntProperty(DbProperties.EstimateNumKeys, out ulong v); sum += v; } }));
        }));
        list.Add(S("property.int-memtable", "property", PathKind.Warm, "TryGetIntProperty(rocksdb.cur-size-all-mem-tables)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Memtable);
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.TryGetIntProperty(DbProperties.CurSizeAllMemTables, out ulong v); sum += v; } }));
        }));
        list.Add(S("property.string", "property", PathKind.Warm, "GetProperty(rocksdb.estimate-num-keys) (string result)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetProperty(DbProperties.EstimateNumKeys)!.Length; }));
        }));
        list.Add(S("property.num-files-at-level", "property", PathKind.Warm, "GetProperty(rocksdb.num-files-at-level6)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetProperty(DbProperties.NumFilesAtLevelPrefix + "6")!.Length; }));
        }, notes: "The string concatenation of the property name is in the scenario (caller); a constant name allocates only the result."));
        list.Add(S("property.stats", "property", PathKind.Cold, "GetProperty(rocksdb.stats) (multi-KB text dump)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetProperty(DbProperties.Stats)!.Length; }));
        }, chunk: 10));
        list.Add(S("property.map", "property", PathKind.Cold, "GetMapProperty(rocksdb.cfstats)", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Cached);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetMapProperty(DbProperties.CfStats)!.Count; }));
        }, chunk: 10));
        list.Add(S("metadata.livefiles", "property", PathKind.Warm, "GetLiveFilesMetadata() on 8 SST files", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetLiveFilesMetadata().Count; }));
        }, chunk: 100));
        list.Add(S("metadata.livefiles-refill", "property", PathKind.Warm, "GetLiveFilesMetadata(reused list) on 8 SST files", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            var files = new List<LiveFileMetadata>();
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.GetLiveFilesMetadata(files); sum += files.Count; } }));
        }, chunk: 100, notes: "One LiveFileMetadata record (with its key copies and name) per file by design."));
        list.Add(S("metadata.visit-livefiles", "property", PathKind.Warm, "VisitLiveFiles(state, static visitor) on 8 SST files", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            var total = new System.Runtime.CompilerServices.StrongBox<ulong>();
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++) db.VisitLiveFiles(total, static (in LiveFileInfo f, System.Runtime.CompilerServices.StrongBox<ulong> t) => t.Value += f.Size);
            }));
        }, chunk: 100));
        list.Add(S("metadata.approximate-sizes-span", "property", PathKind.Warm, "GetApproximateSizes(ranges, Span<ulong>) of 1 KeyRange on 8 SST files", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            KeyRange[] ranges = [new KeyRange(ScenarioContext.Key(1000), ScenarioContext.Key(50_000))];
            var sizes = new ulong[1];
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { db.GetApproximateSizes(ranges, sizes); sum += sizes[0]; } }));
        }, chunk: 100));
        list.Add(S("metadata.approximate-sizes", "property", PathKind.Warm, "GetApproximateSizes(1 KeyRange) on 8 SST files", ctx =>
        {
            RocksDatabase db = ManyFilesDb(ctx);
            KeyRange[] ranges = [new KeyRange(ScenarioContext.Key(1000), ScenarioContext.Key(50_000))];
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetApproximateSizes(ranges)[0]; }));
        }, chunk: 100));
        list.Add(S("metadata.latest-sequence", "property", PathKind.Hot, "GetLatestSequenceNumber()", ctx =>
        {
            (RocksDatabase db, _) = ReadDb(ctx, Location.Memtable);
            ulong sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.GetLatestSequenceNumber(); }));
        }, chunk: 100_000));
        list.Add(S("statistics.read", "property", PathKind.Warm, "Statistics.GetTickerCount + GetHistogramData(DbGet)", ctx =>
        {
            var stats = new Statistics();
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached, configure: (d, _) => d.Statistics = stats);
            ulong sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    sum += stats.GetTickerCount(Ticker.NumberKeysRead);
                    sum += (ulong)stats.GetHistogramData(Histogram.DbGet).Count;
                }
            }));
        }));
        list.Add(S("perfcontext.tostring", "property", PathKind.Cold, "PerfContext.Current.ToString(excludeZeroCounters: true) after one Get", ctx =>
        {
            (RocksDatabase db, byte[][] keys) = ReadDb(ctx, Location.Cached);
            byte[] buf = new byte[256];
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                PerfContext.Level = PerfLevel.EnableCount;
                for (int i = 0; i < n; i++)
                {
                    PerfContext.Current.Reset();
                    db.TryGet(keys[i % keys.Length], buf, out _);
                    sum += PerfContext.Current.ToString(excludeZeroCounters: true).Length;
                }
                PerfContext.Level = PerfLevel.Disable;
            }));
        }, chunk: 100));
    }

    /// <summary>A DB with 8 L0 files of 10k keys each (auto compaction off).</summary>
    internal static RocksDatabase ManyFilesDb(ScenarioContext ctx)
    {
        ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
        o.DisableAutoCompactions = true;
        RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
        byte[] v = ScenarioContext.Value(100);
        for (int f = 0; f < 8; f++)
        {
            ScenarioContext.Fill(db, ScenarioContext.Keys(10_000, f * 10_000), v, flush: true, compact: false);
        }
        return db;
    }

    // ------------------------------------------------------------------------------------------- transactions

    private static void AddTransactions(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(100);
        const int KeySpace = 100_000;
        var wo = new WriteOptions();

        list.Add(S("txn.pessimistic.reuse", "txn", PathKind.Hot, "BeginTransaction(wo, opts, old) + 4 Put + Commit", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var opts = new TransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.pessimistic.new", "txn", PathKind.Hot, "using BeginTransaction(wo) + 4 Put + Commit (new object per txn)", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Transaction txn = t.BeginTransaction(wo);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.pessimistic.getforupdate", "txn", PathKind.Hot, "reused txn: TryGetForUpdate + Put + Commit", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            ScenarioContext.Fill(t.Database, keys, value, flush: true, compact: false);
            var opts = new TransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            var writer = new ArrayBufferWriter<byte>(256);
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    byte[] k = keys[c.Next()];
                    writer.ResetWrittenCount();
                    txn.TryGetForUpdate(null, k, writer);
                    txn.Put(k, value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.pessimistic.get", "txn", PathKind.Hot, "TryGet(null, key, writer) inside an open txn with 10 writes", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            ScenarioContext.Fill(t.Database, keys, value, flush: true, compact: false);
            Transaction txn = ctx.Own(t.BeginTransaction(wo));
            for (int j = 0; j < 10; j++) txn.Put(keys[j * 13], value);
            var writer = new ArrayBufferWriter<byte>(256);
            byte[][] sk = ScenarioContext.Shuffled(keys);
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { writer.ResetWrittenCount(); txn.TryGet(null, sk[c.Next()], writer); } }));
        }));
        list.Add(S("txn.pessimistic.async", "txn", PathKind.Hot, "reused txn: 4 await PutAsync + await CommitAsync", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var opts = new TransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            var c = new Cursor(KeySpace);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    for (int j = 0; j < 4; j++) await txn.PutAsync(keys[c.Next()], value);
                    await txn.CommitAsync();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.2pc", "txn", PathKind.Hot, "reused txn: SetName + 4 Put + Prepare + Commit", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var opts = new TransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            string[] names = new string[64];
            for (int i = 0; i < names.Length; i++) names[i] = "xid-" + i;
            var c = new Cursor(KeySpace);
            int x = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    txn.SetName(names[x++ & 63]);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Prepare();
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.optimistic.reuse", "txn", PathKind.Hot, "optimistic BeginTransaction(wo, opts, old) + 4 Put + Commit", ctx =>
        {
            OptimisticTransactionDatabase t = ctx.Own(OptimisticTransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var opts = new OptimisticTransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.optimistic.history", "txn", PathKind.Hot, "optimistic reused txn: 4 Put + Commit, 2 history memtables to check", ctx =>
        {
            var cfo = ScenarioContext.DefaultCfOptions();
            cfo.WriteBufferSize = 256UL << 20;
            cfo.MaxWriteBufferSizeToMaintain = 1L << 30;
            OptimisticTransactionDatabase t = ctx.Own(OptimisticTransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions(),
                new OptimisticTransactionDbOptions(), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)]));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            for (int f = 0; f < 2; f++)
            {
                for (int i = 0; i < 100; i++) t.Database.Put(keys[i], value);
                t.Database.Flush();
            }
            var opts = new OptimisticTransactionOptions();
            Transaction txn = ctx.Own(t.BeginTransaction(wo, opts));
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    txn = t.BeginTransaction(wo, opts, txn);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.optimistic.new", "txn", PathKind.Hot, "optimistic using BeginTransaction(wo) + 4 Put + Commit", ctx =>
        {
            OptimisticTransactionDatabase t = ctx.Own(OptimisticTransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(KeySpace);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using Transaction txn = t.BeginTransaction(wo);
                    for (int j = 0; j < 4; j++) txn.Put(keys[c.Next()], value);
                    txn.Commit();
                }
            }));
        }, chunk: 250));
        list.Add(S("txn.iterator", "txn", PathKind.Warm, "txn.NewIterator(null) + Seek + 10 Next + Dispose (txn with 10 writes)", ctx =>
        {
            TransactionDatabase t = ctx.Own(TransactionDatabase.Open(ctx.NewDirectory("db"), ScenarioContext.DefaultDbOptions()));
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            ScenarioContext.Fill(t.Database, keys, value, flush: true, compact: false);
            Transaction txn = ctx.Own(t.BeginTransaction(wo));
            for (int j = 0; j < 10; j++) txn.Put(keys[j * 13], value);
            var c = new Cursor(KeySpace);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using TransactionIterator it = txn.NewIterator(null);
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid; j++, it.Next()) sum += it.Key.Length;
                }
            }));
        }));
    }
}
