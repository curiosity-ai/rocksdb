using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Blob reads of one column family: the blob cache (optional <c>blob_cache</c>) in front of the blob file readers.
/// Port of rocksdb::BlobSource (db/blob/blob_source.cc: GetBlob, GetBlobFromCache, PutBlobIntoCache, the
/// kBlockCacheTier Incomplete status), BlobFileCache (db/blob/blob_file_cache.cc: one reader per blob file, created on
/// first use, evicted when the file becomes obsolete) and Version::GetBlob (db/version_set.cc: the blob file must be part
/// of the reading version).
/// <para>
/// Cache entries hold the uncompressed blob, keyed by <c>OffsetableCacheKey(db_id, db_session_id, file_number)
/// .WithOffset(offset)</c> like upstream. Open readers are bounded like upstream's BlobFileCache (which shares the
/// table cache, capacity <c>max_open_files - 10</c>): when a new reader would exceed the capacity, the least recently
/// used readers not in use are closed (they are reopened on demand). Deviation: the bound applies to the blob readers
/// of the column family on their own, not to table and blob readers together. Readers in use are reference counted, so
/// an eviction never closes a reader under an in-flight read; obsolete blob files are evicted right away.
/// </para>
/// Thread-safe.
/// </summary>
internal sealed class BlobSource : IDisposable
{
    private readonly IFileSystem _fs;
    private readonly string _blobDir;
    private readonly uint _columnFamilyId;
    private readonly BlockCache? _cache;
    private readonly string _dbId;
    private readonly string _dbSessionId;
    private readonly Statistics? _stats;
    private readonly ConcurrentDictionary<ulong, ReaderEntry> _readers = new();
    private readonly int _capacity;
    // Base cache keys per blob file (OffsetableCacheKey derivation hashes the ids: computed once per file).
    private readonly ConcurrentDictionary<ulong, OffsetableCacheKey> _baseKeys = new();
    private readonly Lock _openLock = new();
    private bool _disposed;

    /// <param name="maxOpenFiles">DbOptions.max_open_files (-1 = readers stay open until their file is obsolete).</param>
    public BlobSource(IFileSystem fs, string blobDir, uint columnFamilyId, BlockCache? blobCache, string dbId, string dbSessionId, Statistics? stats,
        int maxOpenFiles)
    {
        // table_cache_size = max_open_files - kNumNonTableCacheFiles (db/db_impl/db_impl.cc), as in TableCache.
        _capacity = maxOpenFiles < 0 ? int.MaxValue : Math.Max(maxOpenFiles - 10, 20);
        _fs = fs;
        _blobDir = blobDir;
        _columnFamilyId = columnFamilyId;
        _cache = blobCache;
        _dbId = dbId;
        _dbSessionId = dbSessionId;
        _stats = stats;
    }

    /// <summary>The blob cache, or null.</summary>
    public BlockCache? Cache => _cache;

    /// <summary>
    /// InternalStats::GetBlobFileReadHist(): the column family's blob file read latency histogram, given to the readers
    /// (BlobFileCache's blob_file_read_hist_). Only filled when the DB has statistics.
    /// </summary>
    public HistogramImpl? BlobFileReadHistogram { get; init; }

    /// <summary>
    /// Blob direct write of the column family: blob indexes from memtables may reference its files before a flush
    /// registers them in a version (BlobFilePartitionManager::ResolveBlobDirectWriteIndex). Null without direct write.
    /// </summary>
    public BlobDirectWriteManager? DirectWrite { get; set; }

    /// <summary>Number of open blob file readers.</summary>
    public int OpenReaders => _readers.Count;

    private CacheKey CacheKeyFor(ulong fileNumber, ulong offset)
    {
        if (!_baseKeys.TryGetValue(fileNumber, out OffsetableCacheKey baseKey))
        {
            baseKey = new OffsetableCacheKey(_dbId, _dbSessionId, fileNumber);
            _baseKeys[fileNumber] = baseKey;
        }
        return baseKey.WithOffset(offset);
    }

    /// <summary>
    /// An open reader with the number of reads using it. <see cref="Dead"/> = removed from the cache: the reader is
    /// closed by whoever drops the last reference.
    /// </summary>
    private sealed class ReaderEntry(BlobFileReader reader)
    {
        public readonly BlobFileReader Reader = reader;
        public int Refs;
        public int Dead;
        public long LastUse;
        private int _closed;

        public void Close()
        {
            if (Interlocked.Exchange(ref _closed, 1) == 0) Reader.Dispose();
        }

        /// <summary>Marks the entry dead; closes the reader now if it is not in use.</summary>
        public void Kill()
        {
            Interlocked.Exchange(ref Dead, 1); // full fence before reading Refs (pairs with Acquire's increment)
            if (Volatile.Read(ref Refs) == 0) Close();
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref Refs) == 0 && Volatile.Read(ref Dead) != 0) Close();
        }
    }

    /// <summary>BlobFileCache::GetBlobFileReader: a referenced reader; the caller releases it.</summary>
    private ReaderEntry AcquireReader(ulong fileNumber)
    {
        if (_readers.TryGetValue(fileNumber, out ReaderEntry? e))
        {
            Interlocked.Increment(ref e.Refs);
            if (Volatile.Read(ref e.Dead) == 0)
            {
                Volatile.Write(ref e.LastUse, Environment.TickCount64);
                return e;
            }
            e.Release(); // evicted concurrently: open it again
        }
        lock (_openLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_readers.TryGetValue(fileNumber, out e) && Volatile.Read(ref e.Dead) == 0)
            {
                Interlocked.Increment(ref e.Refs);
                Volatile.Write(ref e.LastUse, Environment.TickCount64);
                return e;
            }
            if (_readers.Count >= _capacity) EvictUnused(_readers.Count - _capacity + 1);
            e = new ReaderEntry(BlobFileReader.Open(_fs, FileNames.BlobFileName(_blobDir, fileNumber), _columnFamilyId, _stats, BlobFileReadHistogram))
            {
                Refs = 1,
                LastUse = Environment.TickCount64,
            };
            _readers[fileNumber] = e;
            return e;
        }
    }

    /// <summary>Closes up to <paramref name="count"/> least recently used readers that are not in use. REQUIRES: _openLock.</summary>
    private void EvictUnused(int count)
    {
        // Cold path (only when the capacity is reached): a linear scan per eviction round.
        while (count-- > 0)
        {
            ulong victim = 0;
            ReaderEntry? oldest = null;
            foreach (KeyValuePair<ulong, ReaderEntry> kv in _readers)
            {
                ReaderEntry c = kv.Value;
                if (Volatile.Read(ref c.Refs) != 0) continue;
                if (oldest is null || Volatile.Read(ref c.LastUse) < Volatile.Read(ref oldest.LastUse))
                {
                    oldest = c;
                    victim = kv.Key;
                }
            }
            if (oldest is null) return; // every reader is in use: exceed the capacity temporarily
            _readers.TryRemove(victim, out _);
            oldest.Kill();
        }
    }

    /// <summary>BlobFileCache::Evict: closes the reader of an obsolete blob file.</summary>
    public void Evict(ulong fileNumber)
    {
        lock (_openLock)
        {
            if (_readers.TryRemove(fileNumber, out ReaderEntry? e)) e.Kill();
        }
        _baseKeys.TryRemove(fileNumber, out _);
    }

    /// <summary>
    /// Resolves the blob index <paramref name="blobIndex"/> of <paramref name="userKey"/> read from
    /// <paramref name="version"/> (Version::GetBlob): appends the value to <paramref name="output"/>. Inlined indexes
    /// (stacked BlobDB TTL entries) yield their value. Throws Corruption for malformed indexes, files not in the version and
    /// damaged blobs; Incomplete when <c>read_tier</c> is kBlockCacheTier and the blob is not cached.
    /// </summary>
    public void GetBlob(VersionStorageInfo version, ReadOptions? options, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndex, IBufferWriter<byte> output)
    {
        BlobIndex index = DecodeChecked(version, blobIndex, out bool directWrite);
        if (index.IsInlined)
        {
            output.Write(index.InlinedValue);
            return;
        }
        if (directWrite)
        {
            GetDirectWriteBlob(options, userKey, index, output);
            return;
        }
        GetBlob(options, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, output);
    }

    /// <summary>
    /// ResolveBlobDirectWriteIndex fallback: a blob of a direct-write file the reading version does not know yet (blob
    /// cache first, then the file itself).
    /// </summary>
    private void GetDirectWriteBlob(ReadOptions? options, ReadOnlySpan<byte> userKey, BlobIndex index, IBufferWriter<byte> output)
    {
        if (TryGetFromCache(index.FileNumber, index.Offset, output)) return;
        if (!DirectWrite!.TryGetBlob(options, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, output))
        {
            ThrowHelper.Corruption("Invalid blob file number " + index.FileNumber.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Async <see cref="GetBlob(VersionStorageInfo, ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{byte}, IBufferWriter{byte})"/>; completes synchronously on a cache hit.</summary>
    public ValueTask GetBlobAsync(VersionStorageInfo version, ReadOptions? options, ReadOnlyMemory<byte> userKey, ReadOnlySpan<byte> blobIndex,
        IBufferWriter<byte> output, CancellationToken cancellationToken)
    {
        BlobIndex index = DecodeChecked(version, blobIndex, out bool directWrite);
        if (index.IsInlined)
        {
            output.Write(index.InlinedValue);
            return ValueTask.CompletedTask;
        }
        if (directWrite)
        {
            GetDirectWriteBlob(options, userKey.Span, index, output);
            return ValueTask.CompletedTask;
        }
        if (TryGetFromCache(index.FileNumber, index.Offset, output)) return ValueTask.CompletedTask;
        CheckCanRead(options);
        return ReadAndCacheAsync(options, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, output, cancellationToken);
    }

    private BlobIndex DecodeChecked(VersionStorageInfo version, ReadOnlySpan<byte> blobIndex, out bool directWrite)
    {
        directWrite = false;
        string? err = BlobIndex.TryDecode(blobIndex, out BlobIndex index);
        if (err is not null) ThrowHelper.Corruption(err);
        if (index.IsInlined) return index;
        if (index.HasTtl) ThrowHelper.Corruption("Unexpected TTL blob file");
        if (version.GetBlobFileMetaData(index.FileNumber) is null)
        {
            // Only files still owned by the blob direct write path fall back to direct reads; once the version knows the
            // file, every result of the version-backed read stands.
            if (DirectWrite is { } dw && dw.IsDirectWriteFile(index.FileNumber))
            {
                directWrite = true;
                return index;
            }
            ThrowHelper.Corruption("Invalid blob file number " + index.FileNumber.ToString(CultureInfo.InvariantCulture));
        }
        return index;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void CheckCanRead(ReadOptions? options)
    {
        if (options?.ReadTier == ReadTier.BlockCacheTier)
        {
            throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Cannot read blob(s): no disk I/O allowed");
        }
    }

    /// <summary>BlobSource::GetBlob for a decoded reference (no version check). Returns the bytes read from the file (0 on a cache hit).</summary>
    public ulong GetBlob(ReadOptions? options, ReadOnlySpan<byte> userKey, ulong fileNumber, ulong offset, ulong size, CompressionType compression,
        IBufferWriter<byte> output)
    {
        if (TryGetFromCache(fileNumber, offset, output)) return 0;
        CheckCanRead(options);
        bool verify = options?.VerifyChecksums ?? true;
        ReaderEntry entry = AcquireReader(fileNumber);
        try
        {
            BlobFileReader reader = entry.Reader;
            if (_cache is null || options?.FillCache == false)
            {
                return reader.GetBlob(userKey, offset, size, compression, verify, output);
            }
            PooledBufferWriter tmp = ObjectPools.RentWriter();
            try
            {
                ulong read = reader.GetBlob(userKey, offset, size, compression, verify, tmp);
                output.Write(tmp.WrittenSpan);
                InsertIntoCache(fileNumber, offset, tmp.WrittenSpan);
                return read;
            }
            finally
            {
                ObjectPools.ReturnWriter(tmp);
            }
        }
        finally
        {
            entry.Release();
        }
    }

    /// <summary>
    /// Partial read of an uncompressed blob of a version's blob file (lazy wide columns): copies the value bytes
    /// [<paramref name="rangeOffset"/>, + <paramref name="destination"/>.Length) without checksum verification and
    /// without populating the blob cache. Incomplete when <c>read_tier</c> is kBlockCacheTier.
    /// </summary>
    public ulong ReadBlobRange(ReadOptions? options, ReadOnlySpan<byte> userKey, ulong fileNumber, ulong offset, ulong size, ulong rangeOffset,
        Span<byte> destination)
    {
        CheckCanRead(options);
        ReaderEntry entry = AcquireReader(fileNumber);
        try
        {
            return entry.Reader.ReadValueRange(userKey.Length, offset, size, rangeOffset, destination);
        }
        finally
        {
            entry.Release();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ReadAndCacheAsync(ReadOptions? options, ReadOnlyMemory<byte> userKey, ulong fileNumber, ulong offset, ulong size,
        CompressionType compression, IBufferWriter<byte> output, CancellationToken cancellationToken)
    {
        bool verify = options?.VerifyChecksums ?? true;
        ReaderEntry entry = AcquireReader(fileNumber);
        try
        {
            BlobFileReader reader = entry.Reader;
            if (_cache is null || options?.FillCache == false)
            {
                await reader.GetBlobAsync(userKey, offset, size, compression, verify, output, cancellationToken).ConfigureAwait(false);
                return;
            }
            PooledBufferWriter tmp = ObjectPools.RentWriter();
            try
            {
                await reader.GetBlobAsync(userKey, offset, size, compression, verify, tmp, cancellationToken).ConfigureAwait(false);
                output.Write(tmp.WrittenSpan);
                InsertIntoCache(fileNumber, offset, tmp.WrittenSpan);
            }
            finally
            {
                ObjectPools.ReturnWriter(tmp);
            }
        }
        finally
        {
            entry.Release();
        }
    }

    /// <summary>BlobSource::GetBlobFromCache: copies a cached blob into <paramref name="output"/>.</summary>
    private bool TryGetFromCache(ulong fileNumber, ulong offset, IBufferWriter<byte> output)
    {
        BlockCache? cache = _cache;
        if (cache is null) return false;
        if (cache.TryLookup(CacheKeyFor(fileNumber, offset), out CachedBlock block))
        {
            try
            {
                output.Write(block.Span);
                _stats?.RecordTick(Ticker.BlobDbCacheHit);
                _stats?.RecordTick(Ticker.BlobDbCacheBytesRead, (ulong)block.Length);
            }
            finally
            {
                block.Dispose();
            }
            return true;
        }
        _stats?.RecordTick(Ticker.BlobDbCacheMiss);
        return false;
    }

    /// <summary>
    /// BlobSource::PutBlobIntoCache / BlobFileBuilder::PutBlobIntoCacheIfNeeded: inserts the uncompressed blob with
    /// bottom priority. No-op without a blob cache.
    /// </summary>
    public void InsertIntoCache(ulong fileNumber, ulong offset, ReadOnlySpan<byte> blob)
    {
        BlockCache? cache = _cache;
        if (cache is null) return;
        byte[] buf = ArrayPool<byte>.Shared.Rent(Math.Max(1, blob.Length));
        blob.CopyTo(buf);
        if (cache.Insert(CacheKeyFor(fileNumber, offset), CacheValue.FromPooledBuffer(buf, blob.Length), blob.Length, CachePriority.Bottom))
        {
            _stats?.RecordTick(Ticker.BlobDbCacheAdd);
            _stats?.RecordTick(Ticker.BlobDbCacheBytesWrite, (ulong)blob.Length);
        }
        else
        {
            _stats?.RecordTick(Ticker.BlobDbCacheAddFailures);
        }
    }

    /// <summary>True if the blob is in the blob cache (BlobSource::TEST_BlobInCache).</summary>
    internal bool IsInCache(ulong fileNumber, ulong offset)
    {
        if (_cache is null || !_cache.TryLookup(CacheKeyFor(fileNumber, offset), out CachedBlock b)) return false;
        b.Dispose();
        return true;
    }

    public void Dispose()
    {
        lock (_openLock)
        {
            _disposed = true;
            foreach (ReaderEntry e in _readers.Values) e.Kill();
            _readers.Clear();
        }
    }
}

/// <summary>
/// Entity blob column fetcher for reads (Version::GetBlob through the column family's <see cref="BlobSource"/>): the
/// BlobFetcher of the read paths (GetContext, DBIter).
/// </summary>
internal readonly struct VersionBlobFetcher(BlobSource source, VersionStorageInfo version, ReadOptions? options) : Wide.IEntityBlobFetcher
{
    public void FetchBlob(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndex, IBufferWriter<byte> output) =>
        source.GetBlob(version, options, userKey, blobIndex, output);
}
