using RocksDb.Managed.Env;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// <see cref="IKeyValueEngine"/> over the managed <see cref="RocksDatabase"/>, configured like
/// <see cref="Native.NativeDbFactory"/>: no compression, format_version 6, optional full bloom filter, WAL/sync per
/// <see cref="EngineOptions"/>. Uses the synchronous, allocation-free APIs (Put via a pooled batch, TryGet into a span).
/// Block cache: HyperClock sized by <see cref="EngineOptions.BlockCacheSize"/> (native uses its default LRU of the same size).
/// </summary>
public sealed class ManagedEngine : IKeyValueEngine
{
    private readonly RocksDatabase _db;
    private readonly WriteOptions _writeOptions;

    public ManagedEngine(string path, EngineOptions options)
    {
        if (options.Compression != "none") throw new NotSupportedException("only compression=none is supported");
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = (ulong)options.WriteBufferSize,
            Compression = Compression.CompressionType.None,
        };
        cfo.TableOptions.BlockSize = (ulong)options.BlockSize;
        cfo.TableOptions.FormatVersion = 6;
        if (options.BlockCacheSize > 0) cfo.TableOptions.BlockCache = Cache.BlockCache.CreateDefault(options.BlockCacheSize, options.BlockSize);
        else cfo.TableOptions.NoBlockCache = true;
        if (options.BloomBitsPerKey > 0) cfo.TableOptions.FilterPolicy = "bloomfilter:" + options.BloomBitsPerKey + ":false";
        var dbo = new DbOptions { CreateIfMissing = true, FileSystem = LocalFileSystem.Instance };
        _db = RocksDatabase.Open(path, dbo, cfo);
        _writeOptions = new WriteOptions { Sync = options.Sync, DisableWal = options.DisableWal };
    }

    public string Name => "managed (RocksDb.Managed)";

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Put(_writeOptions, key, value);

    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength) => _db.TryGet(key, valueBuffer, out valueLength);

    public void Delete(ReadOnlySpan<byte> key) => _db.Delete(_writeOptions, key);

    public void Flush() => _db.Flush();

    public void Dispose() => _db.Dispose();
}
