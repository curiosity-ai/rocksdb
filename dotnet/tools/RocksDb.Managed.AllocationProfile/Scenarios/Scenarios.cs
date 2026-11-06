using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.AllocationProfile;

/// <summary>The scenario registry (one partial file per area).</summary>
internal static partial class Scenarios
{
    private static List<Scenario>? s_all;

    public static List<Scenario> All()
    {
        if (s_all is not null) return s_all;
        var list = new List<Scenario>();
        AddWrites(list);
        AddReads(list);
        AddIterators(list);
        AddMisc(list);
        AddTransactions(list);
        AddBackground(list);
        AddNative(list);
        return s_all = list;
    }

    private static Scenario S(string name, string group, PathKind path, string op, Func<ScenarioContext, ValueTask<Runner>> setup,
        int chunk = 1000, string? notes = null, bool native = false) =>
        new() { Name = name, Group = group, Path = path, Op = op, Setup = setup, ChunkOps = chunk, Notes = notes, Native = native };

    private static ValueTask<Runner> Done(Runner r) => new(r);

    // ------------------------------------------------------------------------------------------------ writes

    private static void AddWrites(List<Scenario> list)
    {
        byte[] value = ScenarioContext.Value(ScenarioContext.ValueSize);
        const int KeySpace = 200_000;

        // Single-key writes: keys cycle over 200k so memtables rotate (4 MiB write buffer) and flushes/compactions run
        // in the background during warm-up and measurement.
        list.Add(S("put.sync", "write", PathKind.Hot, "Put(key, value), WAL on", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(keys[c.Next()], value); }));
        }));
        list.Add(S("put.nowal", "write", PathKind.Hot, "Put(WriteOptions{DisableWal}, key, value)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var wo = new WriteOptions { DisableWal = true };
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(wo, keys[c.Next()], value); }));
        }));
        list.Add(S("put.async", "write", PathKind.Hot, "await PutAsync(key, value), WAL on", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.PutAsync(keys[c.Next()], value);
            }));
        }));
        list.Add(S("put.sync-wal-fsync", "write", PathKind.Warm, "Put(WriteOptions.SyncWrites, key, value)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(WriteOptions.SyncWrites, keys[c.Next()], value); }));
        }, chunk: 20));
        list.Add(S("put.async.fsync", "write", PathKind.Warm, "await PutAsync(WriteOptions.SyncWrites, key, value)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.PutAsync(WriteOptions.SyncWrites, keys[c.Next()], value);
            }));
        }, chunk: 20));
        list.Add(S("put.sync.4threads", "write", PathKind.Hot, "Put from 4 threads (group commit), per Put", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            return Done(Runner.Concurrent(4, (w, n) =>
            {
                int start = w * (KeySpace / 4);
                for (int i = 0; i < n; i++) db.Put(keys[start + i % (KeySpace / 4)], value);
                return default;
            }));
        }, chunk: 40_000));
        list.Add(S("put.async.4tasks", "write", PathKind.Hot, "await PutAsync from 4 concurrent tasks, per Put", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            return Done(Runner.Concurrent(4, [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int w, int n) =>
            {
                int start = w * (KeySpace / 4);
                for (int i = 0; i < n; i++) await db.PutAsync(keys[start + i % (KeySpace / 4)], value);
            }));
        }, chunk: 40_000));
        list.Add(S("put.cf", "write", PathKind.Hot, "ColumnFamily.Put(key, value) on a second column family", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            ColumnFamily cf = db.CreateColumnFamily("other", ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) cf.Put(keys[c.Next()], value); }));
        }));
        list.Add(S("put.udt", "write", PathKind.Hot, "Put(key, timestamp, value) with Comparator.BytewiseWithU64Ts", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.Comparator = Comparator.BytewiseWithU64Ts;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            byte[] ts = new byte[8];
            ulong t = 1;
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(ts, t++);
                    db.Put(keys[c.Next()], ts, value);
                }
            }));
        }));
        list.Add(S("delete.sync", "write", PathKind.Hot, "Delete(key)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Delete(keys[c.Next()]); }));
        }));
        list.Add(S("delete.async", "write", PathKind.Hot, "await DeleteAsync(key)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.DeleteAsync(keys[c.Next()]);
            }));
        }));
        list.Add(S("singledelete", "write", PathKind.Hot, "SingleDelete(key)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.SingleDelete(keys[c.Next()]); }));
        }));
        list.Add(S("deleterange", "write", PathKind.Warm, "DeleteRange(begin, begin+10) over existing keys", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(20_000);
            ScenarioContext.Fill(db, keys, value, flush: true, compact: false);
            var c = new Cursor(keys.Length - 10);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) { int k = c.Next(); db.DeleteRange(keys[k], keys[k + 10]); } }));
        }, chunk: 100, notes: "Range tombstones accumulate in the memtable / L0 (fragmented on reads and flush)."));
        list.Add(S("merge.uint64add", "write", PathKind.Hot, "Merge(key, 8-byte operand) with MergeOperator.UInt64Add", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = MergeOperator.UInt64Add;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(10_000);
            byte[] one = new byte[8];
            one[0] = 1;
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Merge(keys[c.Next()], one); }));
        }, notes: "Flushes and compactions run the merge operator in the background (counted)."));
        list.Add(S("merge.user", "write", PathKind.Hot, "Merge(key, 8-byte operand) with a user MergeOperator (Int64 max)", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = new Int64MaxOperator();
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            byte[][] keys = ScenarioContext.Keys(10_000);
            byte[] operand = new byte[8];
            long x = 0;
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(operand, x++);
                    db.Merge(keys[c.Next()], operand);
                }
            }));
        }));
        list.Add(S("putentity", "write", PathKind.Hot, "PutEntity(key, 3 WideColumns)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            WideColumn[] columns = Columns();
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.PutEntity(keys[c.Next()], columns); }));
        }));
        list.Add(S("putentity.async", "write", PathKind.Hot, "await PutEntityAsync(key, 3 WideColumns)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            WideColumn[] columns = Columns();
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++) await db.PutEntityAsync(keys[c.Next()], columns);
            }));
        }));

        // Write batches.
        list.Add(S("batch.pooled", "batch", PathKind.Hot, "CreateWriteBatch + 10 Put + Write + Dispose", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using WriteBatch b = db.CreateWriteBatch();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("batch.new", "batch", PathKind.Hot, "new WriteBatch() + 10 Put + Write (no Dispose)", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    var b = new WriteBatch();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("batch.reuse-clear", "batch", PathKind.Hot, "one WriteBatch: Clear + 10 Put + Write", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            WriteBatch b = ctx.Own(db.CreateWriteBatch());
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    b.Clear();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("batch.async", "batch", PathKind.Hot, "one WriteBatch: Clear + 10 Put + await WriteAsync", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            WriteBatch b = ctx.Own(db.CreateWriteBatch());
            var c = new Cursor(keys.Length);
            return Done(Runner.Async([AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))] async ValueTask (int n) =>
            {
                for (int i = 0; i < n; i++)
                {
                    b.Clear();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    await db.WriteAsync(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("batch.mixed-cf", "batch", PathKind.Hot, "reused batch: 4 Put (2 CFs) + Delete + Merge + DeleteRange + Write", ctx =>
        {
            ColumnFamilyOptions o = ScenarioContext.DefaultCfOptions();
            o.MergeOperator = MergeOperator.UInt64Add;
            RocksDatabase db = ctx.OpenDb("db", cfOptions: o);
            ColumnFamily cf = db.CreateColumnFamily("other", ScenarioContext.DefaultCfOptions());
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            WriteBatch b = ctx.Own(db.CreateWriteBatch());
            byte[] one = new byte[8];
            var c = new Cursor(keys.Length - 2);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    b.Clear();
                    int k = c.Next();
                    b.Put(keys[k], value).Put(cf, keys[k], value).Put(keys[k + 1], value).Put(cf, keys[k + 1], value)
                     .Delete(keys[k + 2]).Merge(keys[k], one).DeleteRange(keys[k + 1], keys[k + 2]);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("wbwi.new", "batch", PathKind.Hot, "new WriteBatchWithIndex + 10 Put + Write + Dispose", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    using var b = new WriteBatchWithIndex();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
        list.Add(S("wbwi.reuse-clear", "batch", PathKind.Hot, "one WriteBatchWithIndex: Clear + 10 Put + Write", ctx =>
        {
            RocksDatabase db = ctx.OpenDb("db");
            byte[][] keys = ScenarioContext.Keys(KeySpace);
            var b = ctx.Own(new WriteBatchWithIndex());
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    b.Clear();
                    for (int j = 0; j < 10; j++) b.Put(keys[c.Next()], value);
                    db.Write(b);
                }
            }));
        }, chunk: 200));
    }

    internal static WideColumn[] Columns() =>
    [
        WideColumn.Default(ScenarioContext.Value(40, 2)),
        new WideColumn("email"u8.ToArray(), ScenarioContext.Value(30, 3)),
        new WideColumn("city"u8.ToArray(), ScenarioContext.Value(20, 4)),
    ];

    /// <summary>Keeps the maximum of little-endian int64 operands (the README's user merge operator).</summary>
    internal sealed class Int64MaxOperator : MergeOperator
    {
        public override string Name => "Int64MaxOperator";

        public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue,
            in MergeOperands operands, IBufferWriter<byte> result)
        {
            long max = hasExistingValue && existingValue.Length == 8 ? BinaryPrimitives.ReadInt64LittleEndian(existingValue) : long.MinValue;
            for (int i = 0; i < operands.Count; i++)
                max = Math.Max(max, BinaryPrimitives.ReadInt64LittleEndian(operands[i]));
            BinaryPrimitives.WriteInt64LittleEndian(result.GetSpan(8), max);
            result.Advance(8);
            return true;
        }
    }
}
