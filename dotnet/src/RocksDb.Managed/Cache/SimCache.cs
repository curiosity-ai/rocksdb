using System.Globalization;
using System.Text;
using RocksDb.Managed.Compression;

namespace RocksDb.Managed.Cache;

/// <summary>
/// A cache wrapper that also simulates a cache of a different capacity: every insert and lookup is mirrored into a
/// key-only LRU cache (charges only, no values) and its hits and misses are counted (and recorded as
/// SIM_BLOCK_CACHE_HIT / SIM_BLOCK_CACHE_MISS when the lookup carries statistics). Port of
/// utilities/simulator_cache/sim_cache.cc (SimCacheImpl, CacheActivityLogger) and include/rocksdb/utilities/sim_cache.h.
/// Use it as a DB's block cache to answer "what hit rate would capacity X give" without allocating X.
/// </summary>
public sealed class SimCache : BlockCache
{
    private readonly BlockCache _target;
    private readonly BlockCache _keyOnlyCache;
    private long _missTimes;
    private long _hitTimes;
    private readonly CacheActivityLogger _logger = new();
    private static readonly CacheValue s_keyOnly = default(CacheValue).WithHelper(CacheItemHelper.Basic(CacheEntryRole.Misc));

    private SimCache(BlockCache simCache, BlockCache cache) : base(extendedLookup: true)
    {
        _keyOnlyCache = simCache;
        _target = cache;
    }

    /// <summary>NewSimCache(cache, sim_capacity, num_shard_bits): the key-only cache is an LRU cache without metadata charge.</summary>
    internal static SimCache Create(BlockCache cache, long simCapacity, int numShardBits)
    {
        ArgumentNullException.ThrowIfNull(cache);
        if (numShardBits >= 20) ThrowHelper.InvalidArgument("num_shard_bits too large"); // upstream returns nullptr
        var keyOnly = new LruCache(new LruCacheOptions
        {
            Capacity = simCapacity, NumShardBits = numShardBits, MetadataChargePolicy = CacheMetadataChargePolicy.DontCharge,
        });
        return new SimCache(keyOnly, cache);
    }

    /// <summary>NewSimCache(sim_cache, cache, num_shard_bits): with a caller-supplied simulation cache.</summary>
    public static SimCache Create(BlockCache simCache, BlockCache cache)
    {
        ArgumentNullException.ThrowIfNull(simCache);
        ArgumentNullException.ThrowIfNull(cache);
        return new SimCache(simCache, cache);
    }

    public override string Name => "SimCache";

    /// <summary>The cache that actually holds the values.</summary>
    public BlockCache Target => _target;

    /// <summary>Capacity of the simulated cache (GetSimCapacity).</summary>
    public long SimCapacity => _keyOnlyCache.Capacity;

    /// <summary>Charge held by the simulated cache (GetSimUsage).</summary>
    public long SimUsage => _keyOnlyCache.Usage;

    /// <summary>Sets the simulated capacity, purging entries as needed (SetSimCapacity).</summary>
    public void SetSimCapacity(long capacity) => _keyOnlyCache.SetCapacity(capacity);

    /// <summary>Lookups that missed the simulated cache (get_miss_counter).</summary>
    public long MissCount => Interlocked.Read(ref _missTimes);

    /// <summary>Lookups that hit the simulated cache (get_hit_counter).</summary>
    public long HitCount => Interlocked.Read(ref _hitTimes);

    /// <summary>Resets the hit and miss counters (reset_counter; the tickers belong to the DB's statistics).</summary>
    public void ResetCounters()
    {
        Interlocked.Exchange(ref _missTimes, 0);
        Interlocked.Exchange(ref _hitTimes, 0);
    }

    /// <summary>"SimCache MISSes: ..., HITs: ..., HITRATE: ..." (SimCacheImpl::ToString).</summary>
    public override string ToString()
    {
        long miss = MissCount, hit = HitCount, lookups = miss + hit;
        var sb = new StringBuilder();
        sb.Append("SimCache MISSes:  ").Append(miss.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("SimCache HITs:    ").Append(hit.ToString(CultureInfo.InvariantCulture)).Append('\n');
        sb.Append("SimCache HITRATE: ").Append((lookups == 0 ? 0 : hit * 100.0f / lookups).ToString("F2", CultureInfo.InvariantCulture)).Append('\n');
        return sb.ToString();
    }

    /// <summary>
    /// Starts logging cache activity to <paramref name="activityLogFile"/> ("LOOKUP - &lt;hex key&gt;" and
    /// "ADD - &lt;hex key&gt; - &lt;charge&gt;" lines); logging stops at <paramref name="maxLoggingSize"/> bytes (0: unlimited)
    /// or on the first write error (StartActivityLogging).
    /// </summary>
    public void StartActivityLogging(string activityLogFile, long maxLoggingSize) => _logger.StartLogging(activityLogFile, maxLoggingSize);

    /// <summary>Stops activity logging (StopActivityLogging).</summary>
    public void StopActivityLogging() => _logger.StopLogging();

    /// <summary>The first error of background logging, or null (GetActivityLoggingStatus).</summary>
    public Exception? ActivityLoggingError => _logger.Error;

    private void HandleLookup(in CacheKey key, Statistics? stats)
    {
        if (_keyOnlyCache.TryLookup(key, out CachedBlock h))
        {
            h.Release(false);
            Interlocked.Increment(ref _hitTimes);
            stats?.RecordTick(Ticker.SimBlockCacheHit);
        }
        else
        {
            Interlocked.Increment(ref _missTimes);
            stats?.RecordTick(Ticker.SimBlockCacheMiss);
        }
        _logger.ReportLookup(key);
    }

    private void HandleInsert(in CacheKey key, long charge, CachePriority priority)
    {
        // The value is for the real cache; the key-only cache gets a placeholder of the same charge.
        if (!_keyOnlyCache.TryLookup(key, out CachedBlock h)) _keyOnlyCache.Insert(key, s_keyOnly, charge, priority);
        else h.Release(false);
        _logger.ReportAdd(key, charge);
    }

    public override bool TryLookup(in CacheKey key, out CachedBlock block)
    {
        HandleLookup(key, null);
        return _target.TryLookup(key, out block);
    }

    public override bool TryLookup(in CacheKey key, CacheItemHelper? helper, object? createContext, CachePriority priority, Statistics? stats,
        out CachedBlock block)
    {
        HandleLookup(key, stats);
        return _target.ExtendedLookup ? _target.TryLookup(key, helper, createContext, priority, stats, out block) : _target.TryLookup(key, out block);
    }

    public override void StartAsyncLookup(AsyncLookupHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        HandleLookup(handle.Key, handle.Statistics);
        _target.StartAsyncLookup(handle);
    }

    public override void WaitAll(ReadOnlySpan<AsyncLookupHandle> handles) => _target.WaitAll(handles);

    public override ValueTask WaitAllAsync(AsyncLookupHandle[] handles, int count, CancellationToken cancellationToken) =>
        _target.WaitAllAsync(handles, count, cancellationToken);

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, out CachedBlock block)
    {
        HandleInsert(key, charge, priority);
        return _target.TryInsert(key, value, charge, priority, out block);
    }

    public override bool TryInsert(in CacheKey key, in CacheValue value, long charge, CachePriority priority, ReadOnlySpan<byte> compressedValue,
        CompressionType compressionType, out CachedBlock block)
    {
        HandleInsert(key, charge, priority);
        return _target.TryInsert(key, value, charge, priority, compressedValue, compressionType, out block);
    }

    public override bool Insert(in CacheKey key, in CacheValue value, long charge, CachePriority priority)
    {
        HandleInsert(key, charge, priority);
        return _target.Insert(key, value, charge, priority);
    }

    public override void Release(ref CachedBlock handle, bool eraseIfLastRef) => _target.Release(ref handle, eraseIfLastRef);

    public override bool Erase(in CacheKey key)
    {
        bool r = _target.Erase(key);
        _keyOnlyCache.Erase(key);
        return r;
    }

    public override void EraseUnreferencedEntries()
    {
        _target.EraseUnreferencedEntries();
        _keyOnlyCache.EraseUnreferencedEntries();
    }

    internal override bool AdmitsCompressedBlocks => _target.AdmitsCompressedBlocks;

    // SimCacheImpl::ApplyToAllEntries: the real cache's entries.
    public override void ApplyToAllEntries<TState>(TState state, CacheEntryVisitor<TState> callback, int averageEntriesPerLock) =>
        _target.ApplyToAllEntries(state, callback, averageEntriesPerLock);

    public override long Capacity => _target.Capacity;

    public override void SetCapacity(long capacity) => _target.SetCapacity(capacity);

    public override bool StrictCapacityLimit => _target.StrictCapacityLimit;

    public override void SetStrictCapacityLimit(bool strictCapacityLimit) => _target.SetStrictCapacityLimit(strictCapacityLimit);

    public override long Usage => _target.Usage;

    public override long PinnedUsage => _target.PinnedUsage;

    public override int Count => _target.Count;

    public override long NewId() => _target.NewId();

    public override bool TryGetSecondaryCacheCapacity(out long capacity) => _target.TryGetSecondaryCacheCapacity(out capacity);

    /// <summary>Port of CacheActivityLogger: appends activity lines under a lock; stops at the size limit or an error.</summary>
    private sealed class CacheActivityLogger
    {
        private readonly Lock _mutex = new();
        private volatile bool _enabled;
        private FileStream? _file;
        private long _maxLoggingSize;
        private Exception? _error;

        public Exception? Error
        {
            get
            {
                lock (_mutex) return _error;
            }
        }

        public void StartLogging(string path, long maxLoggingSize)
        {
            ArgumentException.ThrowIfNullOrEmpty(path);
            lock (_mutex)
            {
                StopLoggingInternal();
                _file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
                _maxLoggingSize = maxLoggingSize;
                _enabled = true;
            }
        }

        public void StopLogging()
        {
            lock (_mutex) StopLoggingInternal();
        }

        public void ReportLookup(in CacheKey key)
        {
            if (!_enabled) return;
            Append("LOOKUP - " + Hex(key) + "\n");
        }

        public void ReportAdd(in CacheKey key, long size)
        {
            if (!_enabled) return;
            Append("ADD - " + Hex(key) + " - " + size.ToString(CultureInfo.InvariantCulture) + "\n");
        }

        // Slice::ToString(true): upper-case hex of the key bytes.
        private static string Hex(in CacheKey key)
        {
            Span<byte> k = stackalloc byte[CacheKey.Size];
            key.WriteTo(k);
            return Convert.ToHexString(k);
        }

        private void Append(string line)
        {
            lock (_mutex)
            {
                if (!_enabled || _file is null) return;
                try
                {
                    byte[] bytes = Encoding.ASCII.GetBytes(line);
                    _file.Write(bytes);
                }
                catch (IOException e)
                {
                    _error ??= e;
                }
                if ((_maxLoggingSize > 0 && _file.Length >= _maxLoggingSize) || _error is not null) StopLoggingInternal();
            }
        }

        private void StopLoggingInternal()
        {
            if (!_enabled) return;
            _enabled = false;
            try
            {
                _file?.Dispose();
            }
            catch (IOException e)
            {
                _error ??= e;
            }
            _file = null;
        }
    }
}
