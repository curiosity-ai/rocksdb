namespace RocksDb.Managed.Cache;

/// <summary>
/// Result of <see cref="PersistentCache.LookupAsync"/>: when <see cref="Found"/>, <see cref="Data"/>[0..<see cref="Length"/>)
/// holds a copy rented from <see cref="System.Buffers.ArrayPool{T}.Shared"/> that the caller returns.
/// </summary>
public readonly record struct PersistentCacheLookupResult(bool Found, byte[] Data, int Length);

/// <summary>
/// A cache of table blocks that may live outside memory (flash), consulted by block-based table readers before reading a
/// block from its file (BlockBasedTableOptions::persistent_cache). Port of include/rocksdb/persistent_cache.h; the
/// table-side logic is table/persistent_cache_helper.cc. Upstream marks this API legacy (the SecondaryCache API is its
/// successor); it is still shipped, so it is ported.
/// <para>
/// Keys are the block cache keys of the blocks (16 bytes). When <see cref="IsCompressed"/>, values are serialized blocks
/// as stored in the file (payload, possibly compressed, plus the 5-byte trailer, verified on read); otherwise
/// uncompressed block contents. Implementations are thread-safe.
/// </para>
/// </summary>
public abstract class PersistentCache
{
    private long _lastId;

    /// <summary>Inserts a page (Insert). The data is copied. Returns false when the page was not stored (e.g. TryAgain).</summary>
    public abstract bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data);

    /// <summary>
    /// Looks a page up (Lookup). On success <paramref name="data"/>[0..<paramref name="length"/>) holds a copy rented from
    /// <see cref="System.Buffers.ArrayPool{T}.Shared"/>; the caller returns it.
    /// </summary>
    public abstract bool TryLookup(ReadOnlySpan<byte> key, out byte[] data, out int length);

    /// <summary>
    /// Async <see cref="TryLookup"/>: a tier that reads from storage does so with async I/O (the managed counterpart of the
    /// async block reads of BlockFetcher, which upstream does not have for the persistent cache). The default completes
    /// synchronously with <see cref="TryLookup"/>.
    /// </summary>
    public virtual ValueTask<PersistentCacheLookupResult> LookupAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken)
    {
        bool found = TryLookup(key.Span, out byte[] data, out int length);
        return new ValueTask<PersistentCacheLookupResult>(new PersistentCacheLookupResult(found, data, length));
    }

    /// <summary>True if the cache stores serialized (possibly compressed, with trailer) blocks (IsCompressed).</summary>
    public abstract bool IsCompressed { get; }

    /// <summary>Statistics per tier, top-down (Stats).</summary>
    public abstract IReadOnlyList<IReadOnlyDictionary<string, double>> Stats();

    /// <summary>Printable options (GetPrintableOptions).</summary>
    public abstract string GetPrintableOptions();

    /// <summary>Returns a new numeric id (NewId), e.g. for clients sharding the same persistent cache.</summary>
    public virtual long NewId() => Interlocked.Increment(ref _lastId);

    /// <summary>
    /// Creates a file-based persistent cache (a <see cref="BlockCacheTier"/>) under <paramref name="path"/> of
    /// <paramref name="size"/> bytes and opens it (NewPersistentCache, utilities/persistent_cache/block_cache_tier.cc).
    /// <paramref name="optimizedForNvm"/> uses 4 writer threads with 4 KiB dispatches (direct I/O is not used by the port).
    /// </summary>
    public static PersistentCacheTier Create(Env.IFileSystem fileSystem, string path, long size, bool optimizedForNvm)
    {
        var config = new PersistentCacheConfig(fileSystem, path, size);
        if (optimizedForNvm)
        {
            // The default settings are optimized for SSD; NVM devices are better written with parallelism.
            config.EnableDirectWrites = true;
            config.WriterQueueDepth = 4;
            config.WriterDispatchSize = 4 * 1024;
        }
        var cache = new BlockCacheTier(config);
        cache.Open();
        return cache;
    }
}
