using ZstdSharp.Unsafe;

namespace RocksDb.Managed.Compression;

/// <summary>
/// A compression dictionary for data blocks (the "rocksdb.compression_dict" meta block). Port of CompressionDict /
/// UncompressionDict (util/compression.h): the raw dictionary bytes plus, for ZSTD, a digested dictionary created once
/// (ZSTD_createCDict at the compression level for writing, ZSTD_createDDict for reading) and shared by all threads.
/// <para>
/// ZSTD dictionaries may be raw content dictionaries (sampled data block bytes, zstd_max_train_bytes = 0) or trained
/// ones (ZDICT format). LZ4 and zlib use the raw bytes as preceding history; Snappy ignores the dictionary (the legacy
/// behavior upstream). Thread-safe. Dispose releases the digested dictionaries (callers must have finished using it).
/// </para>
/// </summary>
public sealed unsafe class CompressionDictionary : IDisposable
{
    private readonly byte[] _dict;
    private readonly object _lock = new();
    private ZSTD_CDict_s* _cdict;
    private int _cdictLevel;
    private ZSTD_DDict_s* _ddict;
    private bool _disposed;

    public CompressionDictionary(byte[] dictionary)
    {
        ArgumentNullException.ThrowIfNull(dictionary);
        _dict = dictionary;
    }

    /// <summary>The serialized dictionary (as stored in the meta block).</summary>
    public ReadOnlySpan<byte> Data => _dict;

    public int Length => _dict.Length;

    /// <summary>ZSTD_createCDict(dict, level) (CompressionDict ctor), created on first use. Null when creation fails.</summary>
    internal ZSTD_CDict_s* GetZstdCDict(int sanitizedLevel)
    {
        ZSTD_CDict_s* c = _cdict;
        if (c != null && _cdictLevel == sanitizedLevel) return c;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_cdict != null && _cdictLevel == sanitizedLevel) return _cdict;
            if (_cdict != null) Methods.ZSTD_freeCDict(_cdict);
            fixed (byte* p = _dict)
            {
                _cdict = Methods.ZSTD_createCDict(p, (nuint)_dict.Length, sanitizedLevel);
            }
            _cdictLevel = sanitizedLevel;
            return _cdict;
        }
    }

    /// <summary>ZSTD_createDDict (UncompressionDict with ROCKSDB_ZSTD_DDICT), created on first use.</summary>
    internal ZSTD_DDict_s* GetZstdDDict()
    {
        ZSTD_DDict_s* d = _ddict;
        if (d != null) return d;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_ddict == null)
            {
                fixed (byte* p = _dict)
                {
                    _ddict = Methods.ZSTD_createDDict(p, (nuint)_dict.Length);
                }
            }
            return _ddict;
        }
    }

    /// <summary>
    /// Port of DecompressorDict::ApproximateMemoryUsage (util/compression.cc, DecompressorDict::Populate): the object, the
    /// dictionary bytes and the digested ZSTD dictionary once it exists. The charge of a dictionary in the block cache.
    /// </summary>
    internal long ApproximateMemoryUsage
    {
        get
        {
            long usage = ObjectOverhead + _dict.Length;
            lock (_lock)
            {
                if (_ddict != null) usage += (long)Methods.ZSTD_sizeof_DDict(_ddict);
            }
            return usage;
        }
    }

    // sizeof(DecompressorDict) stand-in: the object, its lock and the array header.
    private const int ObjectOverhead = 96;

    /// <summary>Creates the digested ZSTD decompression dictionary now rather than on first use.</summary>
    internal void DigestForDecompression() => GetZstdDDict();

    private void Free()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            if (_cdict != null) Methods.ZSTD_freeCDict(_cdict);
            if (_ddict != null) Methods.ZSTD_freeDDict(_ddict);
            _cdict = null;
            _ddict = null;
        }
    }

    public void Dispose()
    {
        Free();
        GC.SuppressFinalize(this);
    }

    ~CompressionDictionary() => Free();
}
