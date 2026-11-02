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
/// .WithOffset(offset)</c> like upstream. Unlike upstream's BlobFileCache, readers are not limited by
/// <c>max_open_files</c>: a reader stays open until its blob file is obsolete or the column family is closed. A reader
/// is only evicted once no version references its file, so in-flight reads (which hold a version) never race with the
/// eviction.
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
    private readonly ConcurrentDictionary<ulong, BlobFileReader> _readers = new();
    // Base cache keys per blob file (OffsetableCacheKey derivation hashes the ids: computed once per file).
    private readonly ConcurrentDictionary<ulong, OffsetableCacheKey> _baseKeys = new();
    private readonly Lock _openLock = new();
    private bool _disposed;

    public BlobSource(IFileSystem fs, string blobDir, uint columnFamilyId, BlockCache? blobCache, string dbId, string dbSessionId, Statistics? stats)
    {
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

    /// <summary>BlobFileCache::GetBlobFileReader.</summary>
    private BlobFileReader GetReader(ulong fileNumber)
    {
        if (_readers.TryGetValue(fileNumber, out BlobFileReader? r)) return r;
        lock (_openLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_readers.TryGetValue(fileNumber, out r)) return r;
            r = BlobFileReader.Open(_fs, FileNames.BlobFileName(_blobDir, fileNumber), _columnFamilyId, _stats);
            _readers[fileNumber] = r;
            return r;
        }
    }

    /// <summary>BlobFileCache::Evict: closes the reader of an obsolete blob file.</summary>
    public void Evict(ulong fileNumber)
    {
        if (_readers.TryRemove(fileNumber, out BlobFileReader? r)) r.Dispose();
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
        BlobIndex index = DecodeChecked(version, blobIndex);
        if (index.IsInlined)
        {
            output.Write(index.InlinedValue);
            return;
        }
        GetBlob(options, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, output);
    }

    /// <summary>Async <see cref="GetBlob(VersionStorageInfo, ReadOptions?, ReadOnlySpan{byte}, ReadOnlySpan{byte}, IBufferWriter{byte})"/>; completes synchronously on a cache hit.</summary>
    public ValueTask GetBlobAsync(VersionStorageInfo version, ReadOptions? options, ReadOnlyMemory<byte> userKey, ReadOnlySpan<byte> blobIndex,
        IBufferWriter<byte> output, CancellationToken cancellationToken)
    {
        BlobIndex index = DecodeChecked(version, blobIndex);
        if (index.IsInlined)
        {
            output.Write(index.InlinedValue);
            return ValueTask.CompletedTask;
        }
        if (TryGetFromCache(index.FileNumber, index.Offset, output)) return ValueTask.CompletedTask;
        CheckCanRead(options);
        return ReadAndCacheAsync(options, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, output, cancellationToken);
    }

    private static BlobIndex DecodeChecked(VersionStorageInfo version, ReadOnlySpan<byte> blobIndex)
    {
        string? err = BlobIndex.TryDecode(blobIndex, out BlobIndex index);
        if (err is not null) ThrowHelper.Corruption(err);
        if (index.IsInlined) return index;
        if (index.HasTtl) ThrowHelper.Corruption("Unexpected TTL blob file");
        if (version.GetBlobFileMetaData(index.FileNumber) is null)
        {
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
        BlobFileReader reader = GetReader(fileNumber);
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ReadAndCacheAsync(ReadOptions? options, ReadOnlyMemory<byte> userKey, ulong fileNumber, ulong offset, ulong size,
        CompressionType compression, IBufferWriter<byte> output, CancellationToken cancellationToken)
    {
        bool verify = options?.VerifyChecksums ?? true;
        BlobFileReader reader = GetReader(fileNumber);
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
            foreach (BlobFileReader r in _readers.Values) r.Dispose();
            _readers.Clear();
        }
    }
}
