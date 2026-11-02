using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks.Native;

/// <summary>
/// Opens native RocksDB instances (rocksdb-sharp) configured comparably to the managed port: no compression,
/// full bloom filter, LRU block cache of a given size, and WAL/sync per <see cref="EngineOptions"/>.
/// </summary>
public static class NativeDbFactory
{
    /// <summary>The objects a native DB needs to stay alive alongside it.</summary>
    public sealed class NativeDb : IDisposable
    {
        public required RocksDbSharp.RocksDb Db { get; init; }
        public required RocksDbSharp.DbOptions Options { get; init; }
        public required RocksDbSharp.WriteOptions WriteOptions { get; init; }
        public required RocksDbSharp.ReadOptions ReadOptions { get; init; }
        public RocksDbSharp.Cache? BlockCache { get; init; }
        public RocksDbSharp.BloomFilterPolicy? Bloom { get; init; }
        public RocksDbSharp.BlockBasedTableOptions? TableOptions { get; init; }

        public void Dispose() => Db.Dispose();
    }

    public static RocksDbSharp.DbOptions CreateOptions(EngineOptions options, out RocksDbSharp.Cache? cache, out RocksDbSharp.BloomFilterPolicy? bloom, out RocksDbSharp.BlockBasedTableOptions table)
    {
        table = new RocksDbSharp.BlockBasedTableOptions()
            .SetBlockSize((ulong)options.BlockSize)
            .SetFormatVersion(6);
        cache = null;
        bloom = null;
        if (options.BlockCacheSize > 0)
        {
            cache = RocksDbSharp.Cache.CreateLru((ulong)options.BlockCacheSize);
            table.SetBlockCache(cache);
        }
        else
        {
            table.SetNoBlockCache(true);
        }
        if (options.BloomBitsPerKey > 0)
        {
            bloom = RocksDbSharp.BloomFilterPolicy.Create(options.BloomBitsPerKey, false);
            table.SetFilterPolicy(bloom);
        }
        if (options.Compression != "none") throw new NotSupportedException("only compression=none is supported");
        return new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetCompression(RocksDbSharp.Compression.No)
            .SetWriteBufferSize((ulong)options.WriteBufferSize)
            .SetTargetFileSizeBase((ulong)options.TargetFileSizeBase)
            .SetMaxBytesForLevelBase((ulong)options.MaxBytesForLevelBase)
            .SetLevel0FileNumCompactionTrigger(options.Level0FileNumCompactionTrigger)
            .SetDisableAutoCompactions(options.DisableAutoCompactions ? 1 : 0)
            .SetBlockBasedTableFactory(table);
    }

    public static NativeDb Open(string path, EngineOptions options)
    {
        RocksDbSharp.DbOptions dbOptions = CreateOptions(options, out RocksDbSharp.Cache? cache, out RocksDbSharp.BloomFilterPolicy? bloom, out RocksDbSharp.BlockBasedTableOptions table);
        RocksDbSharp.Native.Instance.rocksdb_options_set_max_background_jobs(dbOptions.Handle, options.MaxBackgroundJobs);
        var writeOptions = new RocksDbSharp.WriteOptions().SetSync(options.Sync).DisableWal(options.DisableWal ? 1 : 0);
        return new NativeDb
        {
            Db = RocksDbSharp.RocksDb.Open(dbOptions, path),
            Options = dbOptions,
            WriteOptions = writeOptions,
            ReadOptions = new RocksDbSharp.ReadOptions(),
            BlockCache = cache,
            Bloom = bloom,
            TableOptions = table,
        };
    }
}

/// <summary><see cref="IKeyValueEngine"/> over native RocksDB. Gets are zero-copy via ISpanDeserializer.</summary>
public sealed class NativeKeyValueEngine : IKeyValueEngine
{
    private readonly NativeDbFactory.NativeDb _db;
    private readonly CopyDeserializer _copier = new();
    private readonly RocksDbSharp.FlushOptions _flushOptions = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);

    public NativeKeyValueEngine(string path, EngineOptions options) => _db = NativeDbFactory.Open(path, options);

    public string Name => "native (rocksdb-sharp)";

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Db.Put(key, value, null, _db.WriteOptions);

    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength)
    {
        // The deserializer runs synchronously inside Get; it copies into a scratch array we then expose.
        int n = _db.Db.Get(key, _copier, null, _db.ReadOptions);
        if (n == 0)
        {
            valueLength = 0;
            return false;
        }
        valueLength = n - 1;
        _copier.Buffer.AsSpan(0, Math.Min(valueLength, valueBuffer.Length)).CopyTo(valueBuffer);
        return true;
    }

    public void Delete(ReadOnlySpan<byte> key) => _db.Db.Remove(key, null, _db.WriteOptions);

    public void Flush() => _db.Db.Flush(_flushOptions);

    public void WaitForCompaction()
    {
        // rocksdb_wait_for_compact is not exposed by rocksdb-sharp: poll the background work properties.
        while (_db.Db.GetProperty("rocksdb.compaction-pending") != "0" || _db.Db.GetProperty("rocksdb.num-running-compactions") != "0"
               || _db.Db.GetProperty("rocksdb.num-running-flushes") != "0" || _db.Db.GetProperty("rocksdb.mem-table-flush-pending") != "0")
        {
            Thread.Sleep(10);
        }
    }

    public string? GetProperty(string name) => _db.Db.GetProperty(name);

    public void Dispose() => _db.Dispose();

    /// <summary>Returns length+1 (0 = not found) and copies the value into a reusable buffer.</summary>
    private sealed class CopyDeserializer : RocksDbSharp.ISpanDeserializer<int>
    {
        public byte[] Buffer = new byte[4096];

        public int Deserialize(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length > Buffer.Length) Buffer = new byte[buffer.Length * 2];
            buffer.CopyTo(Buffer);
            return buffer.Length + 1;
        }
    }
}
