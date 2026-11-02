using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table.Filters;

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

    /// <summary>The DB and column family options for <paramref name="options"/> (shared with the transaction engines).</summary>
    internal static (DbOptions Db, ColumnFamilyOptions Cf) BuildOptions(EngineOptions options)
    {
        var cfo = new ColumnFamilyOptions
        {
            WriteBufferSize = (ulong)options.WriteBufferSize,
            Compression = options.Compression,
            TargetFileSizeBase = (ulong)options.TargetFileSizeBase,
            MaxBytesForLevelBase = (ulong)options.MaxBytesForLevelBase,
            Level0FileNumCompactionTrigger = options.Level0FileNumCompactionTrigger,
            DisableAutoCompactions = options.DisableAutoCompactions,
        };
        cfo.TableOptions.BlockSize = (ulong)options.BlockSize;
        cfo.TableOptions.FormatVersion = 6;
        if (options.BlockCacheSize <= 0) cfo.TableOptions.NoBlockCache = true;
        else cfo.TableOptions.BlockCache = options.CacheType switch
        {
            BlockCacheKind.HyperClock => Cache.BlockCache.CreateDefault(options.BlockCacheSize, options.BlockSize),
            BlockCacheKind.Lru => Cache.BlockCache.CreateLru(options.BlockCacheSize),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.CacheType, "unknown cache type"),
        };
        if (options.BloomBitsPerKey > 0) cfo.TableOptions.FilterPolicy = new BloomFilterPolicy(options.BloomBitsPerKey);
        var dbo = new DbOptions { CreateIfMissing = true, FileSystem = LocalFileSystem.Instance, MaxBackgroundJobs = options.MaxBackgroundJobs };
        return (dbo, cfo);
    }

    public ManagedEngine(string path, EngineOptions options)
    {
        (DbOptions dbo, ColumnFamilyOptions cfo) = BuildOptions(options);
        _db = RocksDatabase.Open(path, dbo, cfo);
        _writeOptions = new WriteOptions { Sync = options.Sync, DisableWal = options.DisableWal };
        _syncWriteOptions = new WriteOptions { Sync = true };
    }

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

    public void CompactRange() => _db.CompactRange();

    public void WaitForCompaction() => _db.WaitForCompact();

    public string? GetProperty(string name) => _db.GetProperty(name);

    public void Dispose() => _db.Dispose();

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
