using System.Buffers;
using System.Globalization;
using System.Text;

namespace RocksDb.Managed.Cache;

/// <summary>
/// Configuration of the persistent cache tiers. Port of PersistentCacheConfig
/// (utilities/persistent_cache/persistent_cache_tier.h), with upstream defaults.
/// </summary>
public sealed class PersistentCacheConfig
{
    public PersistentCacheConfig(Env.IFileSystem fileSystem, string path, long cacheSize, int writeBufferSize = 1024 * 1024)
    {
        FileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
        Path = path;
        CacheSize = cacheSize;
        WriteBufferSize = writeBufferSize;
        WriterDispatchSize = writeBufferSize;
    }

    public Env.IFileSystem FileSystem { get; }
    /// <summary>Directory of the cache; the cache files live in <c>&lt;path&gt;/cache</c>.</summary>
    public string Path { get; }
    /// <summary>enable_direct_reads (accepted; the port reads through the file system's normal path).</summary>
    public bool EnableDirectReads { get; set; } = true;
    /// <summary>enable_direct_writes (accepted; not used by the port).</summary>
    public bool EnableDirectWrites { get; set; }
    /// <summary>Logical size of the cache (cache_size).</summary>
    public long CacheSize { get; set; }
    /// <summary>Size of one cache file (cache_file_size, default 100 MiB).</summary>
    public int CacheFileSize { get; set; } = 100 * 1024 * 1024;
    /// <summary>Number of writer threads (writer_qdepth).</summary>
    public int WriterQueueDepth { get; set; } = 1;
    /// <summary>Insert asynchronously through a queue (pipeline_writes).</summary>
    public bool PipelineWrites { get; set; } = true;
    /// <summary>Max pending insert bytes before inserts are dropped (max_write_pipeline_backlog_size, 1 GiB).</summary>
    public long MaxWritePipelineBacklogSize { get; set; } = 1L << 30;
    /// <summary>Size of a write buffer (write_buffer_size, 1 MiB).</summary>
    public int WriteBufferSize { get; set; }
    /// <summary>Bytes per file write (writer_dispatch_size).</summary>
    public int WriterDispatchSize { get; set; }
    /// <summary>Whether the cache stores serialized (compressed) blocks (is_compressed).</summary>
    public bool IsCompressed { get; set; } = true;

    /// <summary>write_buffer_count(): (writer_qdepth + 1.2) * cache_file_size / write_buffer_size.</summary>
    public int WriteBufferCount => (int)((WriterQueueDepth + 1.2) * CacheFileSize / WriteBufferSize);

    /// <summary>ValidateSettings: InvalidArgument for inconsistent sizes.</summary>
    public void Validate()
    {
        if (string.IsNullOrEmpty(Path)) ThrowHelper.InvalidArgument("empty or null args");
        if (CacheSize < CacheFileSize || WriteBufferSize >= CacheFileSize || (long)WriteBufferSize * WriteBufferCount < 2L * CacheFileSize)
            ThrowHelper.InvalidArgument("invalid cache size");
        if (WriterQueueDepth == 0 || WriterDispatchSize > WriteBufferSize || WriteBufferSize % WriterDispatchSize != 0)
            ThrowHelper.InvalidArgument("invalid writer settings");
    }

    /// <summary>PersistentCacheConfig::ToString.</summary>
    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.Append("    path: ").Append(Path).Append('\n');
        sb.Append("    enable_direct_reads: ").Append(EnableDirectReads ? 1 : 0).Append('\n');
        sb.Append("    enable_direct_writes: ").Append(EnableDirectWrites ? 1 : 0).Append('\n');
        sb.Append("    cache_size: ").Append(CacheSize.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    cache_file_size: ").Append(CacheFileSize.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    writer_qdepth: ").Append(WriterQueueDepth.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    pipeline_writes: ").Append(PipelineWrites ? 1 : 0).Append('\n');
        sb.Append("    max_write_pipeline_backlog_size: ").Append(MaxWritePipelineBacklogSize.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    write_buffer_size: ").Append(WriteBufferSize.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    writer_dispatch_size: ").Append(WriterDispatchSize.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("    is_compressed: ").Append(IsCompressed ? 1 : 0).Append('\n');
        return sb.ToString();
    }
}

/// <summary>
/// A tier of a persistent cache that may forward to a next (lower) tier. Port of PersistentCacheTier
/// (utilities/persistent_cache/persistent_cache_tier.{h,cc}).
/// </summary>
public abstract class PersistentCacheTier : PersistentCache, IDisposable
{
    private PersistentCacheTier? _nextTier;

    /// <summary>Opens the tier (and the next tiers).</summary>
    public virtual void Open() => _nextTier?.Open();

    /// <summary>Closes the tier (and the next tiers).</summary>
    public virtual void Close() => _nextTier?.Close();

    /// <summary>Reserves space for <paramref name="size"/> bytes (Reserve); pass-through by default.</summary>
    public virtual bool Reserve(long size) => true;

    /// <summary>Erases a key (Erase); not all tiers support it (pass-through).</summary>
    public virtual bool Erase(ReadOnlySpan<byte> key) => true;

    /// <summary>The next (lower) tier.</summary>
    public virtual PersistentCacheTier? NextTier
    {
        get => _nextTier;
        set
        {
            if (_nextTier is not null) ThrowHelper.InvalidArgument("next tier already set");
            _nextTier = value;
        }
    }

    public override IReadOnlyList<IReadOnlyDictionary<string, double>> Stats() =>
        _nextTier?.Stats() ?? Array.Empty<IReadOnlyDictionary<string, double>>();

    /// <summary>"---- next tier -----" blocks of "name: value" lines (PrintStats).</summary>
    public virtual string PrintStats()
    {
        var sb = new StringBuilder();
        foreach (IReadOnlyDictionary<string, double> tier in Stats())
        {
            sb.Append("---- next tier -----\n");
            var keys = new List<string>(tier.Keys);
            keys.Sort(StringComparer.Ordinal);
            foreach (string k in keys) sb.Append(k).Append(": ").Append(tier[k].ToString(CultureInfo.InvariantCulture)).Append('\n');
        }
        return sb.ToString();
    }

    /// <summary>Waits until pending inserts reached the tiers (TEST_Flush).</summary>
    public virtual void Flush() => _nextTier?.Flush();

    public void Dispose()
    {
        Close();
        GC.SuppressFinalize(this);
    }
}

/// <summary>Tiers stacked top-down (PersistentTieredCache): inserts and lookups go to the first tier, which forwards.</summary>
public sealed class PersistentTieredCache : PersistentCacheTier
{
    private readonly List<PersistentCacheTier> _tiers = new();

    /// <summary>Appends a tier at the bottom (AddTier).</summary>
    public void AddTier(PersistentCacheTier tier)
    {
        ArgumentNullException.ThrowIfNull(tier);
        if (_tiers.Count > 0) _tiers[^1].NextTier = tier;
        _tiers.Add(tier);
    }

    private PersistentCacheTier Front => _tiers.Count > 0 ? _tiers[0] : throw new InvalidOperationException("no tiers");

    public override void Open() => Front.Open();

    public override void Close()
    {
        if (_tiers.Count == 0) return;
        Front.Close();
        _tiers.Clear();
    }

    public override bool Erase(ReadOnlySpan<byte> key) => Front.Erase(key);

    public override IReadOnlyList<IReadOnlyDictionary<string, double>> Stats() => Front.Stats();

    public override string PrintStats() => Front.PrintStats();

    public override bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data) => Front.Insert(key, data);

    public override bool TryLookup(ReadOnlySpan<byte> key, out byte[] data, out int length) => Front.TryLookup(key, out data, out length);

    public override ValueTask<PersistentCacheLookupResult> LookupAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken) =>
        Front.LookupAsync(key, cancellationToken);

    public override bool IsCompressed => Front.IsCompressed;

    public override string GetPrintableOptions() => "PersistentTieredCache";

    public override void Flush()
    {
        Front.Flush();
    }

    public override PersistentCacheTier? NextTier
    {
        get => _tiers.Count > 0 ? _tiers[^1].NextTier : null;
        set => _tiers[^1].NextTier = value;
    }
}

/// <summary>
/// In-memory LRU tier (VolatileCacheTier, utilities/persistent_cache/volatile_tier_impl.{h,cc}): an evictable hash
/// table of pages bounded by <c>max_size</c>; evicted pages are pushed to the next tier, misses fall through to it.
/// Erase is not supported (as upstream).
/// </summary>
public sealed class VolatileCacheTier : PersistentCacheTier
{
    private readonly bool _isCompressed;
    private readonly long _maxSize;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, LinkedListNode<(string Key, byte[] Value)>> _index = new(StringComparer.Ordinal);
    private readonly LinkedList<(string Key, byte[] Value)> _lru = new(); // first = most recently used
    private long _size;
    private long _hits, _misses, _inserts, _evicts;

    /// <param name="isCompressed">Whether the tier stores serialized blocks (default true).</param>
    /// <param name="maxSize">Maximum bytes of page data (default unlimited).</param>
    public VolatileCacheTier(bool isCompressed = true, long maxSize = long.MaxValue)
    {
        _isCompressed = isCompressed;
        _maxSize = maxSize;
    }

    public override bool IsCompressed => _isCompressed;

    public override string GetPrintableOptions() => "    cache_type: VolatileCacheTier\n";

    private static string KeyString(ReadOnlySpan<byte> key) => Convert.ToBase64String(key);

    public override bool Insert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
    {
        string k = KeyString(key);
        lock (_lock)
        {
            // Check if we have overshot the limit, if so evict some space.
            _size += data.Length;
            while (_size > _maxSize)
            {
                if (!Evict())
                {
                    // Unable to evict data, we give up so we don't spike read latency (TryAgain).
                    _size -= data.Length;
                    return false;
                }
            }
            if (_index.ContainsKey(k))
            {
                // Block already in cache (TryAgain).
                _size -= data.Length;
                return false;
            }
            _index[k] = _lru.AddFirst((k, data.ToArray()));
            _inserts++;
            return true;
        }
    }

    public override bool TryLookup(ReadOnlySpan<byte> key, out byte[] data, out int length)
    {
        string k = KeyString(key);
        lock (_lock)
        {
            if (_index.TryGetValue(k, out LinkedListNode<(string Key, byte[] Value)>? node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                byte[] v = node.Value.Value;
                data = ArrayPool<byte>.Shared.Rent(Math.Max(1, v.Length));
                v.CopyTo(data, 0);
                length = v.Length;
                _hits++;
                return true;
            }
            _misses++;
        }
        if (NextTier is { } next) return next.TryLookup(key, out data, out length);
        data = [];
        length = 0;
        return false;
    }

    public override bool Erase(ReadOnlySpan<byte> key) => throw new NotSupportedException("VolatileCacheTier does not support Erase");

    // Evict: drop the least recently used page and push it to the next tier. REQUIRES: _lock.
    private bool Evict()
    {
        LinkedListNode<(string Key, byte[] Value)>? last = _lru.Last;
        if (last is null) return false;
        _lru.RemoveLast();
        _index.Remove(last.Value.Key);
        _evicts++;
        NextTier?.Insert(Convert.FromBase64String(last.Value.Key), last.Value.Value);
        _size -= last.Value.Value.Length;
        return true;
    }

    public override IReadOnlyList<IReadOnlyDictionary<string, double>> Stats()
    {
        Dictionary<string, double> stat;
        lock (_lock)
        {
            long lookups = _hits + _misses;
            stat = new Dictionary<string, double>(StringComparer.Ordinal)
            {
                ["persistent_cache.volatile_cache.hits"] = _hits,
                ["persistent_cache.volatile_cache.misses"] = _misses,
                ["persistent_cache.volatile_cache.inserts"] = _inserts,
                ["persistent_cache.volatile_cache.evicts"] = _evicts,
                ["persistent_cache.volatile_cache.hit_pct"] = lookups == 0 ? 0 : 100.0 * _hits / lookups,
                ["persistent_cache.volatile_cache.miss_pct"] = lookups == 0 ? 0 : 100.0 * _misses / lookups,
            };
        }
        var outList = new List<IReadOnlyDictionary<string, double>>(base.Stats()) { stat };
        return outList;
    }
}
