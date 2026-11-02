using System.Buffers;
using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks;

// Managed sync vs async API on the same operation (per op, single task): the async paths should allocate nothing and
// cost about the same as the sync ones when nothing has to wait (memtable / block cache / page cache hits).
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories DbAsync

/// <summary>Where point lookups find their data blocks.</summary>
public enum BlockSource
{
    /// <summary>64 MiB block cache holding every block.</summary>
    BlockCache,
    /// <summary>1 MiB block cache: most lookups read the block from the file (OS page cache).</summary>
    PageCache,
}

/// <summary>Put / TryGet / Seek+Next / MultiGet through the sync and the async API of RocksDb.Managed.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("DbAsync")]
public class DbAsyncBenchmarks
{
    private const int KeyCount = 100_000;
    private const int OpsPerInvoke = 1000;
    private const int KeySize = 16;
    private const int ValueSize = 100;

    [Params(BlockSource.BlockCache, BlockSource.PageCache)]
    public BlockSource Blocks { get; set; }

    private ManagedEngine _engine = null!;
    private RocksDatabase _db = null!;
    private string _dir = "";
    private byte[][] _keys = null!;
    private byte[] _value = null!;
    private readonly ArrayBufferWriter<byte> _buffer = new(256);
    private Iterator _it = null!;
    private MultiGetBatch _batch = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-async-" + Environment.ProcessId);
        long cache = Blocks == BlockSource.BlockCache ? 64L << 20 : 1L << 20;
        _engine = (ManagedEngine)EngineFactory.Open(EngineKind.Managed, _dir, new EngineOptions { BlockCacheSize = cache }, destroyExisting: true);
        _db = _engine.Db;
        var values = new ValueBuffer(301, ValueSize, 0.5);
        _value = values.Data.AsSpan(0, ValueSize).ToArray();
        byte[] key = new byte[KeySize];
        for (int i = 0; i < KeyCount; i++)
        {
            KeyFormat.Make(key, (ulong)i);
            _db.Put(key, _value);
        }
        _db.Flush();
        var rng = new SplitMix64(42);
        _keys = new byte[OpsPerInvoke][];
        for (int i = 0; i < _keys.Length; i++)
        {
            _keys[i] = new byte[KeySize];
            KeyFormat.Make(_keys[i], rng.Uniform(KeyCount));
        }
        _it = _db.NewIterator();
        _batch = new MultiGetBatch(16);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _it.Dispose();
        _batch.Dispose();
        _engine.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public void Put()
    {
        for (int i = 0; i < _keys.Length; i++) _db.Put(_keys[i], _value);
    }

    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public async Task PutAsync()
    {
        for (int i = 0; i < _keys.Length; i++) await _db.PutAsync(_keys[i], _value).ConfigureAwait(false);
    }

    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int TryGet()
    {
        int found = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _buffer.ResetWrittenCount();
            if (_db.TryGet(_keys[i], _buffer)) found++;
        }
        return found;
    }

    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public async Task<int> TryGetAsync()
    {
        int found = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _buffer.ResetWrittenCount();
            if (await _db.TryGetAsync(_keys[i], _buffer).ConfigureAwait(false)) found++;
        }
        return found;
    }

    /// <summary>Seek + 10 Next (per seek).</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public long Seek10()
    {
        long bytes = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _it.Seek(_keys[i]);
            for (int j = 0; j < 10 && _it.Valid; j++)
            {
                bytes += _it.Value.Length;
                _it.Next();
            }
        }
        return bytes;
    }

    /// <summary>SeekAsync + 10 NextAsync (per seek).</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public async Task<long> Seek10Async()
    {
        long bytes = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            await _it.SeekAsync(_keys[i]).ConfigureAwait(false);
            for (int j = 0; j < 10 && _it.Valid; j++)
            {
                bytes += _it.Value.Length;
                await _it.NextAsync().ConfigureAwait(false);
            }
        }
        return bytes;
    }

    /// <summary>MultiGet of 16 keys (per batch).</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke / 16)]
    public int MultiGet16()
    {
        int found = 0;
        for (int i = 0; i + 16 <= _keys.Length; i += 16)
        {
            _batch.Clear();
            for (int j = 0; j < 16; j++) _batch.Add(_keys[i + j]);
            _db.MultiGet(_batch);
            found += _batch.FoundCount;
        }
        return found;
    }

    /// <summary>MultiGetAsync of 16 keys (per batch).</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke / 16)]
    public async Task<int> MultiGet16Async()
    {
        int found = 0;
        for (int i = 0; i + 16 <= _keys.Length; i += 16)
        {
            _batch.Clear();
            for (int j = 0; j < 16; j++) _batch.Add(_keys[i + j]);
            await _db.MultiGetAsync(_batch).ConfigureAwait(false);
            found += _batch.FoundCount;
        }
        return found;
    }
}
