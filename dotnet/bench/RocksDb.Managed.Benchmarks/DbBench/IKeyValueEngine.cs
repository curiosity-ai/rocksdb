using RocksDb.Managed.Compression;
namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// Key-value engine abstraction used by the db_bench-style runner and the head-to-head micro benchmarks so the native
/// RocksDB (via rocksdb-sharp) and the managed port are measured with identical workloads. Every member is safe to call
/// from several threads at once (like the underlying DBs); the objects it creates (batches, iterators, MultiGet
/// contexts) belong to one thread.
/// </summary>
public interface IKeyValueEngine : IDisposable
{
    string Name { get; }

    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>Put that always fsyncs the WAL (fillsync), independently of <see cref="EngineOptions.Sync"/>.</summary>
    void PutSync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    /// <summary>Point lookup. Copies up to <paramref name="valueBuffer"/>.Length bytes; returns false if not found.</summary>
    bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength);

    void Delete(ReadOnlySpan<byte> key);

    IEngineWriteBatch CreateWriteBatch();

    void Write(IEngineWriteBatch batch);

    IEngineIterator NewIterator();

    IMultiGetContext CreateMultiGetContext(int maxKeys, int keySize);

    /// <summary>Flushes memtables to SST files and waits for completion.</summary>
    void Flush();

    /// <summary>Full-range manual compaction of the default column family (DB::CompactRange(nullptr, nullptr)).</summary>
    void CompactRange();

    /// <summary>Waits until no flush or compaction is running or pending (DB::WaitForCompact, default options).</summary>
    void WaitForCompaction();

    /// <summary>DB::GetProperty on the default column family (null if unknown).</summary>
    string? GetProperty(string name);
}

/// <summary>A reusable write batch owned by one thread.</summary>
public interface IEngineWriteBatch : IDisposable
{
    void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);

    void Clear();
}

/// <summary>A forward/backward iterator owned by one thread. Key/Value spans are valid until the next move.</summary>
public interface IEngineIterator : IDisposable
{
    bool Valid { get; }
    ReadOnlySpan<byte> Key { get; }
    ReadOnlySpan<byte> Value { get; }

    void SeekToFirst();

    void SeekToLast();

    void Seek(ReadOnlySpan<byte> key);

    void Next();

    void Prev();
}

/// <summary>Batched point lookups: fill <see cref="Key"/>(0..count-1), then call <see cref="Run"/>.</summary>
public interface IMultiGetContext : IDisposable
{
    Span<byte> Key(int index);

    /// <summary>Looks up the first <paramref name="count"/> keys; returns the number found and the total value bytes.</summary>
    int Run(int count, out long valueBytes);
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

    /// <summary>--cache_type: lru (RocksDB's default, used by all engines by default) or hyperclock.</summary>
    public BlockCacheKind CacheType { get; init; } = BlockCacheKind.Lru;

    /// <summary>--bloom_bits (db_bench default -1 = none; we default to 10 for realistic point lookups).</summary>
    public int BloomBitsPerKey { get; init; } = 10;

    /// <summary>--write_buffer_size (64 MiB).</summary>
    public long WriteBufferSize { get; init; } = 64L << 20;

    /// <summary>--block_size (4 KiB).</summary>
    public int BlockSize { get; init; } = 4096;

    /// <summary>--target_file_size_base (64 MiB).</summary>
    public long TargetFileSizeBase { get; init; } = 64L << 20;

    /// <summary>--max_bytes_for_level_base (256 MiB).</summary>
    public long MaxBytesForLevelBase { get; init; } = 256L << 20;

    /// <summary>--level0_file_num_compaction_trigger (4).</summary>
    public int Level0FileNumCompactionTrigger { get; init; } = 4;

    /// <summary>--disable_auto_compactions.</summary>
    public bool DisableAutoCompactions { get; init; }

    /// <summary>--enable_blob_files: integrated BlobDB (values of at least --min_blob_size bytes go to blob files).</summary>
    public bool EnableBlobFiles { get; init; }

    /// <summary>--min_blob_size (0).</summary>
    public ulong MinBlobSize { get; init; }

    /// <summary>--max_background_jobs (2).</summary>
    public int MaxBackgroundJobs { get; init; } = 2;
    /// <summary>allow_concurrent_memtable_write (managed engine only; native uses its default, true).</summary>
    public bool AllowConcurrentMemtableWrite { get; init; } = true;
    /// <summary>enable_write_thread_adaptive_yield (managed engine only; native uses its default, true).</summary>
    public bool EnableWriteThreadAdaptiveYield { get; init; } = true;

    /// <summary>--max_subcompactions (1).</summary>
    public uint MaxSubcompactions { get; init; } = 1;

    /// <summary>--compression: none, snappy, lz4 or zstd (all levels).</summary>
    public CompressionType Compression { get; init; } = CompressionType.None;
}

/// <summary>Opens engines by name.</summary>
public static class EngineFactory
{
    public static IKeyValueEngine Open(EngineKind engine, string path, EngineOptions options, bool destroyExisting)
    {
        if (destroyExisting && Directory.Exists(path)) Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return engine switch
        {
            EngineKind.RocksDbSharp => new Native.NativeKeyValueEngine(path, options),
            EngineKind.Managed => new ManagedEngine(path, options),
            _ => throw new ArgumentException("engine " + BenchFlags.Name(engine) + " is not an in-process engine"),
        };
    }
}
