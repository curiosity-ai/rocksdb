using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// User iterator (DBIter over a MergingIterator of 1 memtable + 4 overlapping L0 SSTs) vs. the native rocksdb-sharp
/// iterator over the same LSM shape (4 flushed L0 files + 1 memtable). Full scans and Seek + 100 x Next.
/// Steady-state stepping must not allocate.
/// </summary>
[MemoryDiagnoser]
public class IteratorBenchmarks
{
    private const int NumKeys = 200_000;
    private const int NumFiles = 4;
    private const int MemKeys = 20_000;

    /// <summary>Value size: reverse steps used to copy every value (now pinned in its block), so 1 KiB shows the difference.</summary>
    [Params(100, 1024)]
    public int ValueSize { get; set; }

    private MemTable _mem = null!;
    private BenchTableCache _tableCache = null!;
    private Iterator _managed = null!;
    private byte[][] _seekKeys = null!;
    private int _next;

    private string _nativeDir = null!;
    private RocksDbSharp.RocksDb _nativeDb = null!;
    private RocksDbSharp.Iterator _nativeIter = null!;
    private RocksDbSharp.DbOptions _nativeOptions = null!;
    private RocksDbSharp.BlockBasedTableOptions _nativeTableOptions = null!;
    private RocksDbSharp.Cache _nativeCache = null!;

    private static byte[] UserKey(int i) => System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12", System.Globalization.CultureInfo.InvariantCulture));

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(7);
        byte[] value = new byte[ValueSize];
        InternalKeyComparator icmp = InternalKeyComparator.Bytewise;

        // ---- native: 4 flushed L0 files (key i goes to file i % 4) + one memtable of overwrites.
        _nativeDir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-iterbench-" + Guid.NewGuid().ToString("N"));
        _nativeCache = RocksDbSharp.Cache.CreateLru(1UL << 30);
        _nativeTableOptions = new RocksDbSharp.BlockBasedTableOptions().SetBlockCache(_nativeCache).SetFormatVersion(6);
        _nativeOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1)
            .SetCompression(RocksDbSharp.Compression.No).SetWriteBufferSize(256UL << 20).SetBlockBasedTableFactory(_nativeTableOptions);
        _nativeDb = RocksDbSharp.RocksDb.Open(_nativeOptions, _nativeDir);
        var noWal = new RocksDbSharp.WriteOptions().DisableWal(1);

        // ---- managed: the same shape built with the managed table builder and memtable.
        _tableCache = new BenchTableCache();
        var l0 = new List<FileMetaData>();
        ulong seq = 1;
        for (int f = 0; f < NumFiles; f++)
        {
            var file = new MemWritableFile();
            byte[]? smallest = null, largest = null;
            using (var b = new BlockBasedTableBuilder(file, TableFormatOptions.Default, new TableBuilderOptions()))
            {
                for (int i = f; i < NumKeys; i += NumFiles)
                {
                    rnd.NextBytes(value);
                    byte[] uk = UserKey(i);
                    byte[] k = new byte[uk.Length + 8];
                    DbFormat.AppendInternalKey(k, uk, seq++, EntryType.Value);
                    b.Add(k, value);
                    smallest ??= k;
                    largest = k;
                    _nativeDb.Put(uk, value, null, noWal);
                }
                b.Finish();
            }
            _nativeDb.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
            ulong number = (ulong)f + 1;
            _tableCache.Add(number, file.ToArray());
            l0.Insert(0, new FileMetaData { FileNumber = number, Smallest = smallest!, Largest = largest! });
        }
        _mem = new MemTable(icmp, new MemTableOptions { WriteBufferSize = 256L << 20 }, seq);
        for (int i = 0; i < MemKeys; i++)
        {
            rnd.NextBytes(value);
            byte[] uk = UserKey(rnd.Next(NumKeys));
            _mem.Add(seq++, EntryType.Value, uk, value);
            _nativeDb.Put(uk, value, null, noWal);
        }

        _managed = DbIteratorFactory.Create(new DbIteratorSources
        {
            Comparator = icmp,
            Sequence = seq,
            MemTables = [_mem],
            Levels = [l0],
            TableCache = _tableCache,
        });
        _nativeIter = _nativeDb.NewIterator();

        _seekKeys = new byte[1024][];
        for (int i = 0; i < _seekKeys.Length; i++) _seekKeys[i] = UserKey(rnd.Next(NumKeys));

        // Warm caches.
        ManagedFullScan();
        NativeFullScan();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _managed.Dispose();
        _mem.Dispose();
        _tableCache.Dispose();
        _nativeIter.Dispose();
        _nativeDb.Dispose();
        GC.KeepAlive(_nativeOptions);
        GC.KeepAlive(_nativeTableOptions);
        GC.KeepAlive(_nativeCache);
        try { Directory.Delete(_nativeDir, true); } catch (IOException) { }
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public long ManagedFullScan()
    {
        long n = 0;
        Iterator it = _managed;
        for (it.SeekToFirst(); it.Valid; it.Next()) n += it.Key.Length + it.Value.Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public long NativeFullScan()
    {
        long n = 0;
        RocksDbSharp.Iterator it = _nativeIter;
        for (it.SeekToFirst(); it.Valid(); it.Next()) n += it.GetKeySpan().Length + it.GetValueSpan().Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public long ManagedForeachScan()
    {
        long n = 0;
        foreach (KeyValueRef kv in _managed) n += kv.Key.Length + kv.Value.Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public async Task<long> ManagedAsyncScan()
    {
        long n = 0;
        await foreach (KeyValue kv in _managed.ScanAsync(null, null)) n += kv.Key.Length + kv.Value.Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public long ManagedReverseScan()
    {
        long n = 0;
        Iterator it = _managed;
        for (it.SeekToLast(); it.Valid; it.Prev()) n += it.Key.Length + it.Value.Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public long NativeReverseScan()
    {
        long n = 0;
        RocksDbSharp.Iterator it = _nativeIter;
        for (it.SeekToLast(); it.Valid(); it.Prev()) n += it.GetKeySpan().Length + it.GetValueSpan().Length;
        return n;
    }

    [Benchmark(OperationsPerInvoke = NumKeys)]
    public async Task<long> ManagedAsyncReverseScan()
    {
        long n = 0;
        Iterator it = _managed;
        await it.SeekToLastAsync();
        while (it.Valid)
        {
            n += it.Key.Length + it.Value.Length;
            await it.PrevAsync();
        }
        return n;
    }

    /// <summary>SeekForPrev + 100 x Prev (seekrandom --reverse_iterator), per seek.</summary>
    [Benchmark]
    public long ManagedSeekForPrevPrev100()
    {
        long n = 0;
        Iterator it = _managed;
        it.SeekForPrev(_seekKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && it.Valid; i++, it.Prev()) n += it.Value.Length;
        return n;
    }

    [Benchmark]
    public async Task<long> ManagedSeekForPrevPrev100Async()
    {
        long n = 0;
        Iterator it = _managed;
        await it.SeekForPrevAsync(_seekKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && it.Valid; i++)
        {
            n += it.Value.Length;
            await it.PrevAsync();
        }
        return n;
    }

    [Benchmark]
    public long NativeSeekForPrevPrev100()
    {
        long n = 0;
        RocksDbSharp.Iterator it = _nativeIter;
        it.SeekForPrev(_seekKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && it.Valid(); i++, it.Prev()) n += it.GetValueSpan().Length;
        return n;
    }

    /// <summary>Seek + 100 x Next (seekrandom-style), per seek.</summary>
    [Benchmark]
    public long ManagedSeekNext100()
    {
        long n = 0;
        Iterator it = _managed;
        it.Seek(_seekKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && it.Valid; i++, it.Next()) n += it.Value.Length;
        return n;
    }

    [Benchmark]
    public long NativeSeekNext100()
    {
        long n = 0;
        RocksDbSharp.Iterator it = _nativeIter;
        it.Seek(_seekKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && it.Valid(); i++, it.Next()) n += it.GetValueSpan().Length;
        return n;
    }

    private sealed class BenchTableCache : ITableCache, IDisposable
    {
        private readonly Dictionary<ulong, BlockBasedTableReader> _readers = new();
        private readonly SimpleBlockCache _cache = new();

        public void Add(ulong number, byte[] data) =>
            _readers[number] = BlockBasedTableReader.Open(new MemFile(data), new TableReaderOptions { BlockCache = _cache });

        public ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default) => new(GetReader(file));
        public TableReaderLease GetReader(FileMetaData file) => new(_readers[file.FileNumber], null, null);
        public InternalIterator NewIterator(FileMetaData file) => new TableInternalIterator(_readers[file.FileNumber]);
        public void Evict(ulong fileNumber) { }

        public void Dispose()
        {
            foreach (BlockBasedTableReader r in _readers.Values) r.Dispose();
        }
    }

    private sealed class MemFile(byte[] data) : IRandomAccessFile
    {
        public long Length => data.Length;
        public int Read(long offset, Span<byte> destination)
        {
            int n = (int)Math.Min(destination.Length, data.Length - offset);
            data.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }
        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) => new(Read(offset, destination.Span));
        public void Dispose() { }
    }

    private sealed class MemWritableFile : IWritableFile
    {
        private readonly MemoryStream _ms = new();
        public long Size => _ms.Length;
        public byte[] ToArray() => _ms.ToArray();
        public void Append(ReadOnlySpan<byte> data) => _ms.Write(data);
        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) { _ms.Write(data.Span); return ValueTask.CompletedTask; }
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public void Flush() { }
        public void Sync() { }
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Minimal lock-protected cache (the real block cache is wired by another stream).</summary>
    private sealed class SimpleBlockCache : IBlockCache
    {
        private readonly Dictionary<BlockCacheKey, Block> _map = new();
        private readonly Lock _lock = new();
        public bool TryGet(in BlockCacheKey key, [NotNullWhen(true)] out Block? block)
        {
            lock (_lock)
            {
                if (_map.TryGetValue(key, out block)) { block.AddRef(); return true; }
            }
            return false;
        }
        public void Insert(in BlockCacheKey key, Block block)
        {
            lock (_lock) { if (_map.TryAdd(key, block)) block.AddRef(); }
        }
    }
}
