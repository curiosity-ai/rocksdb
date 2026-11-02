using System.Buffers;
using System.Buffers.Binary;
using RocksDb.Managed;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Table.Filters;
using static QuickStart.Show;

namespace QuickStart;

/// <summary>Opening a DB, typed options, column families and the point-read API family.</summary>
internal static class Basics
{
    public static Task OpenSync(string path)
    {
        // <snippet:open-sync>
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });

        db.Put("hello"u8, "world"u8);                       // ReadOnlySpan<byte> in, no allocation
        byte[]? value = db.Get("hello"u8);                  // simplest read: allocates the result
        Console.WriteLine(System.Text.Encoding.UTF8.GetString(value!));   // world

        db.Delete("hello"u8);
        Console.WriteLine(db.ContainsKey("hello"u8));       // False
        // </snippet:open-sync>
        Check(!db.ContainsKey("hello"u8), "deleted key is gone");
        return Task.CompletedTask;
    }

    public static async Task OpenAsync(string path)
    {
        // <snippet:open-async>
        await using RocksDatabase db = await RocksDatabase.OpenAsync(path, new DbOptions { CreateIfMissing = true });

        // Async writes join the group commit; they complete when the batch is in the WAL and memtable.
        await db.PutAsync("user:1"u8.ToArray(), "alice"u8.ToArray());
        byte[]? alice = await db.GetAsync("user:1"u8.ToArray());   // completes synchronously on cache hits

        await db.FlushAsync();                                      // memtable -> L0 SST
        await db.CompactRangeAsync();                               // full manual compaction
        // </snippet:open-async>
        Line("user:1 = " + Utf8(alice));
        Check(Utf8(alice) == "alice", "async round trip");
    }

    public static Task TypedOptions(string path)
    {
        // <snippet:typed-options>
        var dbOptions = new DbOptions
        {
            CreateIfMissing = true,
            CreateMissingColumnFamilies = true,
            MaxBackgroundJobs = 4,
            WalRecoveryMode = WalRecoveryMode.PointInTimeRecovery,   // enums, not strings
        };
        var cfOptions = new ColumnFamilyOptions
        {
            WriteBufferSize = 32UL << 20,
            Compression = CompressionType.LZ4,
            BottommostCompression = CompressionType.Zstd,
            CompactionStyle = CompactionStyle.Level,
            TableOptions = new BlockBasedTableOptions
            {
                FormatVersion = 7,
                BlockSize = 16 * 1024,
                FilterPolicy = new BloomFilterPolicy(10),            // or new RibbonFilterPolicy(10, 0)
                BlockCache = BlockCache.CreateLru(256L << 20),       // or BlockCache.CreateHyperClock(...)
                CacheIndexAndFilterBlocks = true,
            },
        };

        using RocksDatabase db = RocksDatabase.Open(path, dbOptions, cfOptions);

        // Options are plain readable objects; they are persisted to the native-compatible OPTIONS-xxxxxx file.
        Console.WriteLine(db.DefaultColumnFamily.Options.Compression);   // LZ4
        DbOptions live = db.GetDBOptions();                              // a copy of the DB-wide options

        // Mutable options can be changed on a live DB, typed or with native option names.
        db.SetOptions(db.DefaultColumnFamily, [new("disable_auto_compactions", "true")]);
        db.DefaultColumnFamily.SetOptions(o => o.Level0FileNumCompactionTrigger = 8);
        // </snippet:typed-options>
        Line($"compression={db.DefaultColumnFamily.Options.Compression}, max_background_jobs={live.MaxBackgroundJobs}, " +
             $"L0 trigger={db.DefaultColumnFamily.Options.Level0FileNumCompactionTrigger}");
        Check(db.DefaultColumnFamily.Options.DisableAutoCompactions, "SetOptions applied");
        return Task.CompletedTask;
    }

    public static async Task ColumnFamilies(string path)
    {
        // <snippet:column-families>
        var options = new DbOptions { CreateIfMissing = true };
        using (RocksDatabase db = RocksDatabase.Open(path, options))
        {
            ColumnFamily users = db.CreateColumnFamily("users", new ColumnFamilyOptions());
            users.Put("u1"u8, "alice"u8);                    // the column family is the receiver
            await users.PutAsync("u2"u8.ToArray(), "bob"u8.ToArray());
            db.Put("config"u8, "v1"u8);                      // db.* = default column family
        }

        // Reopen: like native RocksDB, every existing column family must be listed.
        List<string> names = RocksDatabase.ListColumnFamilies(path);            // [default, users]
        var descriptors = new List<ColumnFamilyDescriptor>();
        foreach (string name in names) descriptors.Add(new ColumnFamilyDescriptor(name, new ColumnFamilyOptions()));
        using (RocksDatabase db = RocksDatabase.Open(path, options, descriptors))
        {
            ColumnFamily users = db.GetColumnFamily("users");
            Console.WriteLine(users.Get("u1"u8) is not null);                   // True
            db.DropColumnFamily(users);
        }
        // </snippet:column-families>
        Line("column families before drop: " + string.Join(",", names));
        Check(names.Count == 2, "two column families");
    }

    public static Task ReadVariants(string path)
    {
        using RocksDatabase db = RocksDatabase.Open(path, new DbOptions { CreateIfMissing = true });
        Span<byte> counterValue = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(counterValue, 42);
        db.Put("counter"u8, counterValue);
        db.Put("name"u8, "rocks"u8);

        // <snippet:read-variants>
        // 1. Copy into a caller-owned span (no allocation). False = not found; valueLength is the full length,
        //    so valueLength > buffer.Length means the copy was truncated.
        Span<byte> buffer = stackalloc byte[64];
        if (db.TryGet("name"u8, buffer, out int valueLength) && valueLength <= buffer.Length)
            Console.WriteLine(System.Text.Encoding.UTF8.GetString(buffer[..valueLength]));

        // 2. Append into any IBufferWriter<byte> (ArrayBufferWriter, PooledBufferWriter, a pipe...).
        var writer = new ArrayBufferWriter<byte>();
        bool found = db.TryGet("name"u8, writer);

        // 3. Zero-copy callback: the span points into the memtable or the cached block. Use static lambdas.
        long counter = db.Get("counter"u8, 0, static (value, _) => BinaryPrimitives.ReadInt64LittleEndian(value));

        // 4. Pooled buffer: dispose returns it to the pool.
        using (PooledValue? pooled = db.GetPooled("name"u8))
            if (pooled is not null) Console.WriteLine(pooled.Length);

        // 5. byte[] when convenience matters more than allocation.
        byte[]? copy = db.Get("name"u8);

        // Each variant also takes ReadOptions (snapshot, fill-cache, ...) and has an async twin:
        // TryGetAsync(key, writer), GetPooledAsync(key), GetAsync(key).
        // </snippet:read-variants>
        Line($"found={found} counter={counter} copy={Utf8(copy)}");
        Check(found && counter == 42 && Utf8(copy) == "rocks", "read variants");
        return Task.CompletedTask;
    }
}
