using System.Reflection;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>
/// <see cref="IKeyValueEngine"/> over the managed <see cref="RocksDatabase"/>, configured like
/// <see cref="Native.NativeDbFactory"/> and bench/native-c: compression per <see cref="EngineOptions.Compression"/>,
/// format_version 6, optional full bloom filter, LRU (or HyperClock) block cache, WAL/sync per <see cref="EngineOptions"/>.
/// Uses the synchronous, allocation-free APIs (Put/Write with spans, TryGet into a span, span iterator). The async
/// workloads of the runner use <see cref="Db"/> directly.
/// </summary>
public sealed class ManagedEngine : IKeyValueEngine
{
    private readonly RocksDatabase _db;
    private readonly WriteOptions _writeOptions;
    private readonly WriteOptions _syncWriteOptions;
    private readonly Func<bool>? _compactRange;

    public ManagedEngine(string path, EngineOptions options)
    {
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = (ulong)options.WriteBufferSize,
            Compression = ParseCompression(options.Compression),
        };
        cfo.TableOptions.BlockSize = (ulong)options.BlockSize;
        cfo.TableOptions.FormatVersion = 6;
        if (options.BlockCacheSize <= 0) cfo.TableOptions.NoBlockCache = true;
        else if (options.CacheType == "hyperclock") cfo.TableOptions.BlockCache = Cache.BlockCache.CreateDefault(options.BlockCacheSize, options.BlockSize);
        else if (options.CacheType == "lru") cfo.TableOptions.BlockCache = Cache.BlockCache.CreateLru(options.BlockCacheSize);
        else throw new ArgumentException("unknown --cache_type " + options.CacheType);
        if (options.BloomBitsPerKey > 0) cfo.TableOptions.FilterPolicy = "bloomfilter:" + options.BloomBitsPerKey + ":false";
        var dbo = new DbOptions { CreateIfMissing = true, FileSystem = LocalFileSystem.Instance };
        _db = RocksDatabase.Open(path, dbo, cfo);
        _writeOptions = new WriteOptions { Sync = options.Sync, DisableWal = options.DisableWal };
        _syncWriteOptions = new WriteOptions { Sync = true };
        _compactRange = FindCompactRange(_db);
    }

    public static Compression.CompressionType ParseCompression(string name) => name switch
    {
        "none" or "no" => Compression.CompressionType.None,
        "snappy" => Compression.CompressionType.Snappy,
        "lz4" => Compression.CompressionType.LZ4,
        "zstd" => Compression.CompressionType.Zstd,
        _ => throw new ArgumentException("unsupported --compression " + name + " (none|snappy|lz4|zstd)"),
    };

    public string Name => "managed (RocksDb.Managed)";

    /// <summary>The underlying database (used by the async workloads).</summary>
    public RocksDatabase Db => _db;

    /// <summary>The write options derived from --sync/--disable_wal.</summary>
    public WriteOptions WriteOptions => _writeOptions;

    public WriteOptions SyncWriteOptions => _syncWriteOptions;

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Put(_writeOptions, key, value);

    public void PutSync(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => _db.Put(_syncWriteOptions, key, value);

    public bool TryGet(ReadOnlySpan<byte> key, Span<byte> valueBuffer, out int valueLength) => _db.TryGet(key, valueBuffer, out valueLength);

    public void Delete(ReadOnlySpan<byte> key) => _db.Delete(_writeOptions, key);

    public IEngineWriteBatch CreateWriteBatch() => new Batch(_db.CreateWriteBatch());

    public void Write(IEngineWriteBatch batch) => _db.Write(_writeOptions, ((Batch)batch).Inner);

    public IEngineIterator NewIterator() => new Iter(_db.NewIterator());

    public IMultiGetContext CreateMultiGetContext(int maxKeys, int keySize) => new MultiGet(_db, maxKeys, keySize);

    public void Flush() => _db.Flush();

    public bool TryCompactRange() => _compactRange is not null && _compactRange();

    public void Dispose() => _db.Dispose();

    /// <summary>
    /// Compaction is being implemented concurrently; bind to a full-range <c>CompactRange</c>/<c>CompactRangeAsync</c>
    /// overload by reflection when one exists (parameters that are nullable references, <see cref="CompactRangeOptions"/>
    /// or a <see cref="CancellationToken"/>), so the benchmark picks it up without code changes.
    /// </summary>
    private static Func<bool>? FindCompactRange(RocksDatabase db)
    {
        foreach (string name in new[] { "CompactRange", "CompactRangeAsync" })
        {
            foreach (MethodInfo m in typeof(RocksDatabase).GetMethods(BindingFlags.Public | BindingFlags.Instance))
            {
                if (m.Name != name || m.IsGenericMethodDefinition) continue;
                ParameterInfo[] ps = m.GetParameters();
                var args = new object?[ps.Length];
                bool ok = true;
                for (int i = 0; i < ps.Length && ok; i++)
                {
                    Type t = ps[i].ParameterType;
                    if (t == typeof(CancellationToken)) args[i] = CancellationToken.None;
                    else if (t == typeof(CompactRangeOptions)) args[i] = new CompactRangeOptions();
                    else if (!t.IsValueType) args[i] = null;
                    else if (Nullable.GetUnderlyingType(t) is not null) args[i] = null;
                    else ok = false; // spans etc. cannot be passed through reflection
                }
                if (!ok) continue;
                MethodInfo method = m;
                return () =>
                {
                    object? r = method.Invoke(db, args);
                    if (r is ValueTask vt) vt.AsTask().GetAwaiter().GetResult();
                    else if (r is Task task) task.GetAwaiter().GetResult();
                    return true;
                };
            }
        }
        return null;
    }

    private sealed class Batch(WriteBatch inner) : IEngineWriteBatch
    {
        public WriteBatch Inner => inner;

        public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => inner.Put(key, value);

        public void Clear() => inner.Clear();

        public void Dispose() => inner.Dispose();
    }

    private sealed class Iter(Iterator it) : IEngineIterator
    {
        public bool Valid => it.Valid;
        public ReadOnlySpan<byte> Key => it.Key;
        public ReadOnlySpan<byte> Value => it.Value;

        public void SeekToFirst() => it.SeekToFirst();

        public void SeekToLast() => it.SeekToLast();

        public void Seek(ReadOnlySpan<byte> key) => it.Seek(key);

        public void Next() => it.Next();

        public void Prev() => it.Prev();

        public void Dispose() => it.Dispose();
    }

    /// <summary>
    /// <see cref="RocksDatabase.MultiGet(IReadOnlyList{byte[]})"/> is the managed batched API; it takes a list of key
    /// arrays and returns a new array of value arrays (allocates per call; see the alloc column).
    /// </summary>
    private sealed class MultiGet : IMultiGetContext
    {
        private readonly RocksDatabase _db;
        private readonly byte[][] _keys;
        private readonly PrefixList _list;

        public MultiGet(RocksDatabase db, int maxKeys, int keySize)
        {
            _db = db;
            _keys = new byte[maxKeys][];
            for (int i = 0; i < maxKeys; i++) _keys[i] = new byte[keySize];
            _list = new PrefixList(_keys);
        }

        public Span<byte> Key(int index) => _keys[index];

        public int Run(int count, out long valueBytes)
        {
            _list.Count = count;
            byte[]?[] values = _db.MultiGet(_list);
            int found = 0;
            long bytes = 0;
            for (int i = 0; i < count; i++)
            {
                if (values[i] is { } v)
                {
                    found++;
                    bytes += v.Length;
                }
            }
            valueBytes = bytes;
            return found;
        }

        public void Dispose()
        {
        }
    }

    /// <summary>The first <see cref="Count"/> arrays of a fixed array, without copying.</summary>
    private sealed class PrefixList(byte[][] items) : IReadOnlyList<byte[]>
    {
        public int Count { get; set; } = items.Length;

        public byte[] this[int index] => (uint)index < (uint)Count ? items[index] : throw new ArgumentOutOfRangeException(nameof(index));

        public IEnumerator<byte[]> GetEnumerator()
        {
            for (int i = 0; i < Count; i++) yield return items[i];
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
