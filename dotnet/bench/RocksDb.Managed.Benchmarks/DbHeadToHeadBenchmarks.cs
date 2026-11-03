using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks;

// Head-to-head DB-level micro benchmarks: RocksDb.Managed vs native RocksDB through rocksdb-sharp, on the same
// IKeyValueEngine abstraction the dbbench runner uses (the most efficient span APIs on both sides; the interface call
// costs ~1 ns). Keys/values are generated exactly like dbbench (16-byte keys, 100-byte compressible values).
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --filter '*DbHeadToHead*'
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories Get --join

/// <summary>Opens one engine per <see cref="Engine"/> parameter in a fresh temp directory.</summary>
[MemoryDiagnoser]
public abstract class DbHeadToHeadBase
{
    protected const int KeySize = 16;
    protected const int ValueSize = 100;
    protected const int OpsPerInvoke = 1000;

    [Params(EngineKind.Managed, EngineKind.RocksDbSharp)]
    public EngineKind Engine { get; set; } = EngineKind.Managed;

    protected IKeyValueEngine Db = null!;
    protected ValueBuffer Values = null!;
    private string _dir = "";

    protected void Open(EngineOptions options)
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-h2h-" + GetType().Name + "-" + BenchFlags.Name(Engine) + "-" + Environment.ProcessId);
        Db = EngineFactory.Open(Engine, _dir, options, destroyExisting: true);
        Values = new ValueBuffer(301, ValueSize, 0.5);
    }

    /// <summary>Writes keys 0..count-1 (in a random order) and optionally flushes them to an SST file.</summary>
    protected void Load(int count, bool flush)
    {
        byte[] key = new byte[KeySize];
        var vc = new ValueCursor(Values);
        var rng = new SplitMix64(7);
        var order = new int[count];
        for (int i = 0; i < count; i++) order[i] = i;
        for (int i = count - 1; i > 0; i--)
        {
            int j = (int)rng.Uniform((ulong)(i + 1));
            (order[i], order[j]) = (order[j], order[i]);
        }
        foreach (int k in order)
        {
            KeyFormat.Make(key, (ulong)k);
            Db.Put(key, vc.Next(ValueSize).Span);
        }
        if (flush) Db.Flush();
    }

    /// <summary>Pre-generated random keys in [0, range) so key generation stays out of the measured loop.</summary>
    protected static byte[][] RandomKeys(int count, int range, bool missing)
    {
        var rng = new SplitMix64(42);
        var keys = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            keys[i] = new byte[KeySize];
            KeyFormat.Make(keys[i], rng.Uniform((ulong)range));
            if (missing) keys[i][KeySize - 1] = (byte)'.';
        }
        return keys;
    }

    protected void CloseDb()
    {
        Db?.Dispose();
        try
        {
            if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }
}

[BenchmarkCategory("DbHeadToHead", "Put")]
public class DbPutBenchmarks : DbHeadToHeadBase
{
    private byte[][] _keys = null!;
    private int _valuePos;

    [GlobalSetup]
    public void Setup()
    {
        Open(new EngineOptions());
        _keys = RandomKeys(OpsPerInvoke, 1_000_000, missing: false);
    }

    /// <summary>Put of a random 16-byte key / 100-byte value with the WAL on (no sync).</summary>
    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public void Put()
    {
        byte[] data = Values.Data;
        for (int i = 0; i < _keys.Length; i++)
        {
            if (_valuePos + ValueSize > Values.Length) _valuePos = 0;
            Db.Put(_keys[i], data.AsSpan(_valuePos, ValueSize));
            _valuePos += ValueSize;
        }
    }

    [GlobalCleanup]
    public void Cleanup() => CloseDb();
}

[BenchmarkCategory("DbHeadToHead", "WriteBatch")]
public class DbWriteBatchBenchmarks : DbHeadToHeadBase
{
    private const int BatchCount = 100;
    private byte[][] _keys = null!;
    private IEngineWriteBatch _batch = null!;

    [GlobalSetup]
    public void Setup()
    {
        Open(new EngineOptions());
        _keys = RandomKeys(BatchCount, 1_000_000, missing: false);
        _batch = Db.CreateWriteBatch();
    }

    /// <summary>Clear + 100 Put into a reusable batch + Write (WAL on, no sync). Per batch.</summary>
    [Benchmark(Description = "WriteBatch(100) write")]
    public void Write100()
    {
        _batch.Clear();
        byte[] data = Values.Data;
        for (int i = 0; i < BatchCount; i++) _batch.Put(_keys[i], data.AsSpan(i * ValueSize, ValueSize));
        Db.Write(_batch);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _batch.Dispose();
        CloseDb();
    }
}

public enum GetLocation
{
    /// <summary>All keys in the memtable.</summary>
    Memtable,

    /// <summary>All keys in one SST file whose blocks are all in the (large) block cache.</summary>
    SstCached,

    /// <summary>Absent keys after a flush: answered by the bloom filter.</summary>
    Miss,

    /// <summary>
    /// All keys in one SST file (~13 MB of data blocks) behind a 1 MiB block cache: almost every lookup misses the
    /// block cache and reads its data block from the file (OS page cache), evicting another block. Once the cache is
    /// full, the managed engine reuses the evicted block's buffer, so "Allocated" should stay "-".
    /// </summary>
    SstUncached,
}

[BenchmarkCategory("DbHeadToHead", "Get")]
public class DbGetBenchmarks : DbHeadToHeadBase
{
    private const int KeyCount = 100_000;
    private byte[][] _keys = null!;
    private readonly byte[] _buffer = new byte[ValueSize * 2];

    [Params(GetLocation.Memtable, GetLocation.SstCached, GetLocation.Miss, GetLocation.SstUncached)]
    public GetLocation Location { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        Open(new EngineOptions { BlockCacheSize = Location == GetLocation.SstUncached ? 1L << 20 : 64L << 20 });
        Load(KeyCount, flush: Location != GetLocation.Memtable);
        _keys = RandomKeys(OpsPerInvoke, KeyCount, missing: Location == GetLocation.Miss);
        // Warm the block cache with every data block.
        for (int i = 0; i < KeyCount; i += 16)
        {
            byte[] k = new byte[KeySize];
            KeyFormat.Make(k, (ulong)i);
            Db.TryGet(k, _buffer, out _);
        }
    }

    [Benchmark(OperationsPerInvoke = OpsPerInvoke)]
    public int Get()
    {
        int found = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            if (Db.TryGet(_keys[i], _buffer, out _)) found++;
        }
        return found;
    }

    [GlobalCleanup]
    public void Cleanup() => CloseDb();
}

[BenchmarkCategory("DbHeadToHead", "Iterator")]
public class DbIteratorBenchmarks : DbHeadToHeadBase
{
    private const int KeyCount = 100_000;
    private const int Seeks = 100;
    private byte[][] _keys = null!;
    private IEngineIterator _it = null!;

    [Params(10, 100)]
    public int Nexts { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        Open(new EngineOptions { BlockCacheSize = 64L << 20 });
        Load(KeyCount, flush: true);
        _keys = RandomKeys(Seeks, KeyCount, missing: false);
        _it = Db.NewIterator();
        for (_it.SeekToFirst(); _it.Valid; _it.Next())
        {
        }
    }

    /// <summary>Seek to a random key then Next() <see cref="Nexts"/> times, reading key and value. Per seek.</summary>
    [Benchmark(OperationsPerInvoke = Seeks, Description = "Seek + N x Next")]
    public long SeekNext()
    {
        long bytes = 0;
        for (int i = 0; i < _keys.Length; i++)
        {
            _it.Seek(_keys[i]);
            for (int j = 0; j < Nexts && _it.Valid; j++)
            {
                bytes += _it.Key.Length + _it.Value.Length;
                _it.Next();
            }
        }
        return bytes;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _it.Dispose();
        CloseDb();
    }
}

[BenchmarkCategory("DbHeadToHead", "MultiGet")]
public class DbMultiGetBenchmarks : DbHeadToHeadBase
{
    private const int KeyCount = 100_000;
    private const int BatchKeys = 16;
    private const int Batches = 64;
    private byte[][] _keys = null!;
    private IMultiGetContext _ctx = null!;

    [GlobalSetup]
    public void Setup()
    {
        Open(new EngineOptions { BlockCacheSize = 64L << 20 });
        Load(KeyCount, flush: true);
        _keys = RandomKeys(BatchKeys * Batches, KeyCount, missing: false);
        _ctx = Db.CreateMultiGetContext(BatchKeys, KeySize);
        byte[] buffer = new byte[ValueSize * 2];
        for (int i = 0; i < KeyCount; i += 16)
        {
            byte[] k = new byte[KeySize];
            KeyFormat.Make(k, (ulong)i);
            Db.TryGet(k, buffer, out _);
        }
    }

    /// <summary>MultiGet of 16 random existing keys (SST, cached blocks). Per 16-key batch.</summary>
    [Benchmark(OperationsPerInvoke = Batches, Description = "MultiGet(16)")]
    public long MultiGet16()
    {
        long total = 0;
        for (int b = 0; b < Batches; b++)
        {
            for (int j = 0; j < BatchKeys; j++) _keys[b * BatchKeys + j].CopyTo(_ctx.Key(j));
            total += _ctx.Run(BatchKeys, out _);
        }
        return total;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _ctx.Dispose();
        CloseDb();
    }
}
