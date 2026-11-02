namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// Minimal key-value engine abstraction used by the db_bench-style runner so the native RocksDB (via rocksdb-sharp)
/// and the managed port can be measured with identical workloads. Implementations are used from one thread unless
/// stated otherwise.
/// </summary>
public interface IKeyValueEngine : IDisposable
{
    string Name { get; }

    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>Point lookup. Copies up to <paramref name="valueBuffer"/>.Length bytes; returns false if not found.</summary>
    bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength);

    void Delete(ReadOnlySpan<byte> key);

    /// <summary>Flushes memtables to SST files and waits for completion.</summary>
    void Flush();
}

/// <summary>Engine-independent DB configuration, mirroring the db_bench flags the runner supports.</summary>
public sealed record EngineOptions
{
    /// <summary>--disable_wal</summary>
    public bool DisableWal { get; init; }

    /// <summary>--sync: fsync the WAL on every write.</summary>
    public bool Sync { get; init; }

    /// <summary>--cache_size (db_bench default 8 MiB).</summary>
    public long BlockCacheSize { get; init; } = 8L << 20;

    /// <summary>--bloom_bits (db_bench default -1 = none; we default to 10 for realistic point lookups).</summary>
    public int BloomBitsPerKey { get; init; } = 10;

    /// <summary>--write_buffer_size (64 MiB).</summary>
    public long WriteBufferSize { get; init; } = 64L << 20;

    /// <summary>--block_size (4 KiB).</summary>
    public int BlockSize { get; init; } = 4096;

    /// <summary>Compression is always "none" for now so that compression codecs don't dominate comparisons.</summary>
    public string Compression { get; init; } = "none";
}

/// <summary>Opens engines by name.</summary>
public static class EngineFactory
{
    public static IKeyValueEngine Open(string engine, string path, EngineOptions options, bool destroyExisting)
    {
        if (destroyExisting && Directory.Exists(path)) Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return engine switch
        {
            "native" => new Native.NativeKeyValueEngine(path, options),
            "managed" => new ManagedEngine(path, options),
            _ => throw new ArgumentException("unknown engine '" + engine + "' (expected native|managed)"),
        };
    }
}
