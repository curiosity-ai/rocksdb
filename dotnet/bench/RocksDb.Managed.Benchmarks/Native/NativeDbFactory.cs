using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks.Native;

/// <summary>
/// Opens native RocksDB instances (rocksdb-sharp) configured like the managed engine and bench/native-c: compression
/// per <see cref="EngineOptions.Compression"/>, format_version 6, full bloom filter, LRU (or HyperClock) block cache,
/// and WAL/sync per <see cref="EngineOptions"/>.
/// </summary>
public static class NativeDbFactory
{
    /// <summary>The objects a native DB needs to stay alive alongside it.</summary>
    public sealed class NativeDb : IDisposable
    {
        public required RocksDbSharp.RocksDb Db { get; init; }
        public required RocksDbSharp.DbOptions Options { get; init; }
        public required RocksDbSharp.WriteOptions WriteOptions { get; init; }
        public required RocksDbSharp.WriteOptions SyncWriteOptions { get; init; }
        public required RocksDbSharp.ReadOptions ReadOptions { get; init; }
        public RocksDbSharp.Cache? BlockCache { get; init; }
        public IntPtr HyperClockCache { get; init; }
        public RocksDbSharp.BloomFilterPolicy? Bloom { get; init; }
        public RocksDbSharp.BlockBasedTableOptions? TableOptions { get; init; }

        public void Dispose()
        {
            Db.Dispose();
            if (HyperClockCache != IntPtr.Zero) RocksDbSharp.Native.Instance.rocksdb_cache_destroy(HyperClockCache);
        }
    }

    public static RocksDbSharp.Compression ToNative(Compression.CompressionType compression) => compression switch
    {
        Compression.CompressionType.None => RocksDbSharp.Compression.No,
        Compression.CompressionType.Snappy => RocksDbSharp.Compression.Snappy,
        Compression.CompressionType.LZ4 => RocksDbSharp.Compression.Lz4,
        Compression.CompressionType.Zstd => RocksDbSharp.Compression.Zstd,
        _ => throw new ArgumentException("unsupported compression " + compression),
    };

    public static RocksDbSharp.DbOptions CreateOptions(EngineOptions options, out RocksDbSharp.Cache? cache, out IntPtr hyperClock,
        out RocksDbSharp.BloomFilterPolicy? bloom, out RocksDbSharp.BlockBasedTableOptions table)
    {
        table = new RocksDbSharp.BlockBasedTableOptions()
            .SetBlockSize((ulong)options.BlockSize)
            .SetFormatVersion(6);
        cache = null;
        hyperClock = IntPtr.Zero;
        bloom = null;
        if (options.BlockCacheSize <= 0)
        {
            table.SetNoBlockCache(true);
        }
        else if (options.CacheType == DbBench.BlockCacheKind.HyperClock)
        {
            // rocksdb-sharp has no HyperClock factory; use the C API directly (0 = auto-resizing entry charge).
            hyperClock = RocksDbSharp.Native.Instance.rocksdb_cache_create_hyper_clock((UIntPtr)(ulong)options.BlockCacheSize, UIntPtr.Zero);
            RocksDbSharp.Native.Instance.rocksdb_block_based_options_set_block_cache(table.Handle, hyperClock);
        }
        else if (options.CacheType == DbBench.BlockCacheKind.Lru)
        {
            cache = RocksDbSharp.Cache.CreateLru((ulong)options.BlockCacheSize);
            table.SetBlockCache(cache);
        }
        else
        {
            throw new ArgumentException("unknown --cache_type " + options.CacheType);
        }
        if (options.BloomBitsPerKey > 0)
        {
            bloom = RocksDbSharp.BloomFilterPolicy.Create(options.BloomBitsPerKey, false);
            table.SetFilterPolicy(bloom);
        }
        return new RocksDbSharp.DbOptions()
            .SetCreateIfMissing(true)
            .SetCompression(ToNative(options.Compression))
            .SetWriteBufferSize((ulong)options.WriteBufferSize)
            .SetTargetFileSizeBase((ulong)options.TargetFileSizeBase)
            .SetMaxBytesForLevelBase((ulong)options.MaxBytesForLevelBase)
            .SetLevel0FileNumCompactionTrigger(options.Level0FileNumCompactionTrigger)
            .SetDisableAutoCompactions(options.DisableAutoCompactions ? 1 : 0)
            .SetBlockBasedTableFactory(table);
    }

    public static NativeDb Open(string path, EngineOptions options)
    {
        RocksDbSharp.DbOptions dbOptions = CreateOptions(options, out RocksDbSharp.Cache? cache, out IntPtr hyperClock,
            out RocksDbSharp.BloomFilterPolicy? bloom, out RocksDbSharp.BlockBasedTableOptions table);
        // rocksdb-sharp's DbOptions has no SetMaxBackgroundJobs; use the C API on its handle.
        RocksDbSharp.Native.Instance.rocksdb_options_set_max_background_jobs(dbOptions.Handle, options.MaxBackgroundJobs);
        var writeOptions = new RocksDbSharp.WriteOptions().SetSync(options.Sync).DisableWal(options.DisableWal ? 1 : 0);
        return new NativeDb
        {
            Db = RocksDbSharp.RocksDb.Open(dbOptions, path),
            Options = dbOptions,
            WriteOptions = writeOptions,
            SyncWriteOptions = new RocksDbSharp.WriteOptions().SetSync(true),
            ReadOptions = new RocksDbSharp.ReadOptions(),
            BlockCache = cache,
            HyperClockCache = hyperClock,
            Bloom = bloom,
            TableOptions = table,
        };
    }
}

/// <summary>
/// <see cref="IKeyValueEngine"/> over native RocksDB via rocksdb-sharp, using its most efficient APIs: span overloads
/// for Put/Delete/WriteBatch.Put/Seek, <c>Get(span, ISpanDeserializer)</c> (rocksdb_get + copy, no managed allocation),
/// zero-copy <c>GetKeySpan/GetValueSpan</c> on iterators, and <c>rocksdb_batched_multi_get_cf</c> through
/// <see cref="RocksDbSharp.Native"/> with pinned buffers (rocksdb-sharp's own <c>MultiGet</c> takes <c>byte[][]</c>
/// and returns new arrays).
/// </summary>
public sealed class NativeKeyValueEngine : IKeyValueEngine
{
    private readonly NativeDbFactory.NativeDb _db;
    private readonly RocksDbSharp.FlushOptions _flushOptions = new RocksDbSharp.FlushOptions().SetWaitForFlush(true);
    private readonly IntPtr _defaultCf;

    [ThreadStatic] private static CopyDeserializer? t_copier;

    public NativeKeyValueEngine(string path, EngineOptions options)
    {
        _db = NativeDbFactory.Open(path, options);
        // rocksdb-sharp's GetDefaultColumnFamily() requires opening with column families; use the C API handle.
        _defaultCf = RocksDbSharp.Native.Instance.rocksdb_get_default_column_family_handle(_db.Db.Handle);
    }

    public string Name => "native (rocksdb-sharp)";

    public NativeDbFactory.NativeDb Native => _db;

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Db.Put(key, value, null, _db.WriteOptions);

    public void PutSync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Db.Put(key, value, null, _db.SyncWriteOptions);

    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength)
    {
        // The deserializer runs synchronously inside Get and copies the value into a per-thread scratch array.
        CopyDeserializer copier = t_copier ??= new CopyDeserializer();
        int n = _db.Db.Get(key, copier, null, _db.ReadOptions);
        if (n == 0)
        {
            valueLength = 0;
            return false;
        }
        valueLength = n - 1;
        copier.Buffer.AsSpan(0, Math.Min(valueLength, valueBuffer.Length)).CopyTo(valueBuffer);
        return true;
    }

    public void Delete(ReadOnlySpan<byte> key) => _db.Db.Remove(key, null, _db.WriteOptions);

    public IEngineWriteBatch CreateWriteBatch() => new Batch();

    public void Write(IEngineWriteBatch batch) => _db.Db.Write(((Batch)batch).Inner, _db.WriteOptions);

    public IEngineIterator NewIterator() => new Iter(_db.Db.NewIterator(null, _db.ReadOptions));

    public IMultiGetContext CreateMultiGetContext(int maxKeys, int keySize) => new MultiGet(this, maxKeys, keySize);

    public void Flush() => _db.Db.Flush(_flushOptions);

    public void CompactRange() => _db.Db.CompactRange((byte[]?)null, null);

    public void WaitForCompaction()
    {
        // rocksdb-sharp has no wrapper; call rocksdb_wait_for_compact (default WaitForCompactOptions) directly.
        RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
        IntPtr options = native.rocksdb_wait_for_compact_options_create();
        try
        {
            native.rocksdb_wait_for_compact(_db.Db.Handle, options, out IntPtr err);
            if (err != IntPtr.Zero)
            {
                string msg = Marshal.PtrToStringUTF8(err) ?? "error";
                native.rocksdb_free(err);
                throw new InvalidOperationException("WaitForCompact failed: " + msg);
            }
        }
        finally
        {
            native.rocksdb_wait_for_compact_options_destroy(options);
        }
    }

    public string? GetProperty(string name) => _db.Db.GetProperty(name);

    public void Dispose()
    {
        RocksDbSharp.Native.Instance.rocksdb_column_family_handle_destroy(_defaultCf);
        _db.Dispose();
    }

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

    private sealed class Batch : IEngineWriteBatch
    {
        public RocksDbSharp.WriteBatch Inner { get; } = new();

        public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Inner.Put(key, value);

        public void Clear() => Inner.Clear();

        public void Dispose() => Inner.Dispose();
    }

    private sealed class Iter(RocksDbSharp.Iterator it) : IEngineIterator
    {
        public bool Valid => it.Valid();
        public ReadOnlySpan<byte> Key => it.GetKeySpan();
        public ReadOnlySpan<byte> Value => it.GetValueSpan();

        public void SeekToFirst() => it.SeekToFirst();

        public void SeekToLast() => it.SeekToLast();

        public void Seek(ReadOnlySpan<byte> key) => it.Seek(key);

        public void Next() => it.Next();

        public void Prev() => it.Prev();

        public void Dispose() => it.Dispose();
    }

    /// <summary>
    /// rocksdb_batched_multi_get_cf with keys in a pinned buffer and results as pinnable slices (zero-copy values,
    /// no managed allocation per call), i.e. what bench/native-c does.
    /// </summary>
    private sealed unsafe class MultiGet : IMultiGetContext
    {
        private readonly NativeKeyValueEngine _engine;
        private readonly int _keySize;
        private readonly byte[] _keyBuffer;
        private readonly IntPtr[] _keyPtrs;
        private readonly UIntPtr[] _keySizes;
        private readonly IntPtr[] _values;
        private readonly IntPtr[] _errs;

        public MultiGet(NativeKeyValueEngine engine, int maxKeys, int keySize)
        {
            _engine = engine;
            _keySize = keySize;
            _keyBuffer = GC.AllocateArray<byte>(maxKeys * keySize, pinned: true);
            _keyPtrs = new IntPtr[maxKeys];
            _keySizes = new UIntPtr[maxKeys];
            _values = GC.AllocateArray<IntPtr>(maxKeys, pinned: true);
            _errs = new IntPtr[maxKeys];
            IntPtr basePtr = (IntPtr)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_keyBuffer));
            for (int i = 0; i < maxKeys; i++)
            {
                _keyPtrs[i] = basePtr + i * keySize;
                _keySizes[i] = (UIntPtr)keySize;
            }
        }

        public Span<byte> Key(int index) => _keyBuffer.AsSpan(index * _keySize, _keySize);

        public int Run(int count, out long valueBytes)
        {
            RocksDbSharp.Native native = RocksDbSharp.Native.Instance;
            IntPtr values = (IntPtr)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_values));
            // The keys/sizes arrays are passed as full arrays; only the first `count` entries are read.
            native.rocksdb_batched_multi_get_cf(_engine._db.Db.Handle, _engine._db.ReadOptions.Handle, _engine._defaultCf,
                (UIntPtr)count, _keyPtrs, _keySizes, values, _errs, 0);
            int found = 0;
            long bytes = 0;
            for (int i = 0; i < count; i++)
            {
                if (_errs[i] != IntPtr.Zero)
                {
                    string msg = Marshal.PtrToStringUTF8(_errs[i]) ?? "error";
                    native.rocksdb_free(_errs[i]);
                    _errs[i] = IntPtr.Zero;
                    throw new InvalidOperationException("MultiGet failed: " + msg);
                }
                IntPtr v = _values[i];
                if (v != IntPtr.Zero)
                {
                    native.rocksdb_pinnableslice_value(v, out UIntPtr len);
                    bytes += (long)(ulong)len;
                    found++;
                    native.rocksdb_pinnableslice_destroy(v);
                }
            }
            valueBytes = bytes;
            return found;
        }

        public void Dispose()
        {
        }
    }
}
