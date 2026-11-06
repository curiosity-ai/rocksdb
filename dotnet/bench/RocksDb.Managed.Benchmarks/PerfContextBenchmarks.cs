using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// The hot paths PerfContext instruments (Get from the memtable and from a cached SST block, Seek + 10 x Next, Put)
/// with the perf level off (the default: each instrumentation point must cost nothing measurable) and at
/// EnableCount / EnableTimeExceptForMutex (what turning PerfContext on costs).
/// </summary>
[MemoryDiagnoser]
public class PerfContextBenchmarks
{
    private const int NumKeys = 100_000;
    private const int Ops = 1000;

    /// <summary>The levels to run; PERFCTX_LEVELS=disable restricts the run to the default level (before/after comparisons).</summary>
    [ParamsSource(nameof(Levels))]
    public PerfLevel Level { get; set; }

    public static IEnumerable<PerfLevel> Levels() =>
        Environment.GetEnvironmentVariable("PERFCTX_LEVELS") == "disable"
            ? [PerfLevel.Disable]
            : [PerfLevel.Disable, PerfLevel.EnableCount, PerfLevel.EnableTimeExceptForMutex];

    private static readonly WriteOptions s_noWal = new() { DisableWal = true };

    private RocksDatabase _db = null!;
    private byte[][] _sstKeys = null!;
    private byte[][] _memKeys = null!;
    private byte[] _value = null!;
    private byte[] _buffer = null!;
    private Iterator _it = null!;
    private int _next;
    private string _dir = "";

    private static byte[] Key(int i) => System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D12", System.Globalization.CultureInfo.InvariantCulture));

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-perfctx-bench-" + Environment.ProcessId);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true, FileSystem = LocalFileSystem.Instance },
            new ColumnFamilyOptions { Compression = RocksDb.Managed.Compression.CompressionType.None });
        _value = new byte[100];
        _buffer = new byte[256];
        for (int i = 0; i < NumKeys; i++) _db.Put(Key(i), _value);
        _db.Flush();
        _db.CompactRange(null, null);
        for (int i = 0; i < 10_000; i++) _db.Put(Key(NumKeys + i), _value);
        var rng = new Random(42);
        _sstKeys = new byte[Ops][];
        _memKeys = new byte[Ops][];
        for (int i = 0; i < Ops; i++)
        {
            _sstKeys[i] = Key(rng.Next(NumKeys));
            _memKeys[i] = Key(NumKeys + rng.Next(10_000));
        }
        foreach (byte[] k in _sstKeys) _db.TryGet(k, _buffer, out _); // warm the block cache
        _it = _db.NewIterator();
        // The setup thread is the benchmark thread (in-process and out-of-process toolchains run both on one thread).
        PerfContext.Level = Level;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        PerfContext.Level = PerfLevel.Disable;
        _it.Dispose();
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int GetMemtable()
    {
        int n = 0;
        foreach (byte[] k in _memKeys) n += _db.TryGet(k, _buffer, out int w) ? w : 0;
        return n;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int GetSstCached()
    {
        int n = 0;
        foreach (byte[] k in _sstKeys) n += _db.TryGet(k, _buffer, out int w) ? w : 0;
        return n;
    }

    [Benchmark(OperationsPerInvoke = 100, Description = "Seek + 10 x Next")]
    public int SeekNext()
    {
        int n = 0;
        for (int s = 0; s < 100; s++)
        {
            _it.Seek(_sstKeys[_next++ % Ops]);
            for (int i = 0; i < 10 && _it.Valid; i++)
            {
                n += _it.Key.Length;
                _it.Next();
            }
        }
        return n;
    }

    /// <summary>Put without the WAL (overwrites of 1000 keys: the benchmark must not fill the disk).</summary>
    [Benchmark(OperationsPerInvoke = Ops)]
    public void Put()
    {
        foreach (byte[] k in _memKeys) _db.Put(s_noWal, k, _value);
    }
}
