namespace RocksDb.Managed.AllocationProfile;

/// <summary>
/// rocksdb-sharp (native RocksDB through P/Invoke) comparison: only the managed GC allocations of the wrapper are
/// visible here (native malloc is not counted).
/// </summary>
internal static partial class Scenarios
{
    private sealed class NativeDb(RocksDbSharp.RocksDb db, string dir) : IDisposable
    {
        public RocksDbSharp.RocksDb Db { get; } = db;

        public void Dispose()
        {
            Db.Dispose();
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private static (RocksDbSharp.RocksDb Db, byte[][] Keys) NativeReadDb(ScenarioContext ctx)
    {
        string dir = ctx.NewDirectory("native");
        var table = new RocksDbSharp.BlockBasedTableOptions()
            .SetBlockCache(RocksDbSharp.Cache.CreateLru(64UL << 20))
            .SetFilterPolicy(RocksDbSharp.BloomFilterPolicy.Create(10, false));
        var options = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetBlockBasedTableFactory(table).SetWriteBufferSize(4UL << 20);
        var db = RocksDbSharp.RocksDb.Open(options, dir);
        ctx.Own(new NativeDb(db, dir));
        byte[][] keys = ScenarioContext.Keys(ReadKeys);
        byte[] v = ScenarioContext.Value(100);
        foreach (byte[] k in keys) db.Put(k, v);
        db.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        db.CompactRange((byte[]?)null, null);
        byte[][] sk = ScenarioContext.Shuffled(keys);
        foreach (byte[] k in sk) db.Get(k);
        return (db, sk);
    }

    private static void AddNative(List<Scenario> list)
    {
        list.Add(S("native.get.bytes", "native", PathKind.Hot, "rocksdb-sharp byte[] Get(byte[] key), cached SST", ctx =>
        {
            (RocksDbSharp.RocksDb db, byte[][] keys) = NativeReadDb(ctx);
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()]).Length; }));
        }, native: true));
        list.Add(S("native.get.buffer", "native", PathKind.Hot, "rocksdb-sharp long Get(key, buffer, 0, len), cached SST", ctx =>
        {
            (RocksDbSharp.RocksDb db, byte[][] keys) = NativeReadDb(ctx);
            byte[] buf = new byte[256];
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.Get(keys[c.Next()], buf, 0, buf.Length); }));
        }, native: true));
        list.Add(S("native.put", "native", PathKind.Hot, "rocksdb-sharp Put(ReadOnlySpan key, ReadOnlySpan value), WAL on", ctx =>
        {
            string dir = ctx.NewDirectory("native");
            var db = RocksDbSharp.RocksDb.Open(new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWriteBufferSize(4UL << 20), dir);
            ctx.Own(new NativeDb(db, dir));
            byte[][] keys = ScenarioContext.Keys(200_000);
            byte[] v = ScenarioContext.Value(100);
            var c = new Cursor(keys.Length);
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) db.Put(keys[c.Next()].AsSpan(), v.AsSpan()); }));
        }, native: true));
        list.Add(S("native.iter.seek-next10", "native", PathKind.Hot, "rocksdb-sharp reused Iterator: Seek + 10 x (Key(), Value() byte[]) Next", ctx =>
        {
            (RocksDbSharp.RocksDb db, byte[][] keys) = NativeReadDb(ctx);
            RocksDbSharp.Iterator it = ctx.Own(db.NewIterator());
            var c = new Cursor(keys.Length);
            long sum = 0;
            return Done(Runner.Sync(n =>
            {
                for (int i = 0; i < n; i++)
                {
                    it.Seek(keys[c.Next()]);
                    for (int j = 0; j < 10 && it.Valid(); j++, it.Next()) sum += it.Key().Length + it.Value().Length;
                }
            }));
        }, native: true));
        list.Add(S("native.multiget16", "native", PathKind.Hot, "rocksdb-sharp MultiGet(byte[][16]) (per batch)", ctx =>
        {
            (RocksDbSharp.RocksDb db, byte[][] keys) = NativeReadDb(ctx);
            var groups = new byte[keys.Length / 16][][];
            for (int g = 0; g < groups.Length; g++) groups[g] = keys.AsSpan(g * 16, 16).ToArray();
            var c = new Cursor(groups.Length);
            long sum = 0;
            return Done(Runner.Sync(n => { for (int i = 0; i < n; i++) sum += db.MultiGet(groups[c.Next()]).Length; }));
        }, chunk: 50, native: true));
    }
}
