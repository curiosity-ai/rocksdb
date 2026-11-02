using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using ValueType = RocksDb.Managed.Db.ValueType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Managed MemTable Add / Get / scan vs. native RocksDB as a rough baseline (native: WAL disabled, 1 GiB write buffer,
/// so Put/Get are served by its skiplist memtable; native numbers include P/Invoke and rocksdb-sharp overhead, and
/// native Get allocates the returned byte[]). Keys: 16 bytes, values: 100 bytes. Results are per operation.
/// </summary>
[MemoryDiagnoser]
public class MemTableBenchmarks
{
    private const int N = 100_000;
    private byte[][] _keys = [];
    private byte[] _value = [];
    private MemTable? _mem;
    private MemTable? _filled;
    private string? _nativeDir;
    private RocksDbSharp.RocksDb? _native;
    private RocksDbSharp.RocksDb? _nativeFilled;
    private string? _nativeFilledDir;
    private RocksDbSharp.WriteOptions? _writeOptions;

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(42);
        _keys = new byte[N][];
        for (int i = 0; i < N; i++)
        {
            _keys[i] = new byte[16];
            rnd.NextBytes(_keys[i]);
        }
        _value = new byte[100];
        rnd.NextBytes(_value);
        _filled = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions { WriteBufferSize = 1L << 30 }, 0);
        for (int i = 0; i < N; i++) _filled.Add((ulong)i + 1, ValueType.Value, _keys[i], _value);

        _writeOptions = new RocksDbSharp.WriteOptions().DisableWal(1);
        _nativeFilledDir = NewDir();
        _nativeFilled = RocksDbSharp.RocksDb.Open(NativeOptions(), _nativeFilledDir);
        for (int i = 0; i < N; i++) _nativeFilled.Put(_keys[i], _value, null, _writeOptions);
    }

    private static string NewDir() => Path.Combine(Path.GetTempPath(), "rocksdb-managed-bench-" + Guid.NewGuid().ToString("N"));

    private static RocksDbSharp.DbOptions NativeOptions() =>
        new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetWriteBufferSize(1UL << 30);

    [GlobalCleanup]
    public void Cleanup()
    {
        _filled?.Dispose();
        _nativeFilled?.Dispose();
        TryDelete(_nativeFilledDir);
    }

    private static void TryDelete(string? dir)
    {
        if (dir is null) return;
        try { Directory.Delete(dir, true); } catch (IOException) { }
    }

    [IterationSetup(Target = nameof(ManagedAdd))]
    public void NewMem() => _mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions { WriteBufferSize = 1L << 30 }, 0);

    [IterationCleanup(Target = nameof(ManagedAdd))]
    public void DisposeMem() => _mem?.Dispose();

    [IterationSetup(Target = nameof(NativePut))]
    public void NewNative()
    {
        _nativeDir = NewDir();
        _native = RocksDbSharp.RocksDb.Open(NativeOptions(), _nativeDir);
    }

    [IterationCleanup(Target = nameof(NativePut))]
    public void DisposeNative()
    {
        _native?.Dispose();
        TryDelete(_nativeDir);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public void ManagedAdd()
    {
        MemTable mem = _mem!;
        byte[][] keys = _keys;
        for (int i = 0; i < keys.Length; i++) mem.Add((ulong)i + 1, ValueType.Value, keys[i], _value);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public void NativePut()
    {
        RocksDbSharp.RocksDb db = _native!;
        byte[][] keys = _keys;
        for (int i = 0; i < keys.Length; i++) db.Put(keys[i], _value, null, _writeOptions);
    }

    [Benchmark(OperationsPerInvoke = N)]
    public int ManagedGet()
    {
        MemTable mem = _filled!;
        byte[][] keys = _keys;
        int total = 0;
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        for (int i = 0; i < keys.Length; i++)
        {
            using var lk = new LookupKey(keys[i], DbFormat.MaxSequenceNumber, scratch);
            ulong maxCovering = 0;
            if (mem.Get(in lk, out ReadOnlySpan<byte> v, out _, out _, null, ref maxCovering) == MemTableGetResult.Found) total += v.Length;
        }
        return total;
    }

    [Benchmark(OperationsPerInvoke = N)]
    public int NativeGet()
    {
        RocksDbSharp.RocksDb db = _nativeFilled!;
        byte[][] keys = _keys;
        int total = 0;
        for (int i = 0; i < keys.Length; i++) total += db.Get(keys[i])?.Length ?? 0;
        return total;
    }

    [Benchmark(OperationsPerInvoke = N)]
    public int ManagedScan()
    {
        MemTableIterator it = _filled!.NewIterator();
        int total = 0;
        for (it.SeekToFirst(); it.Valid; it.Next()) total += it.Key.Length + it.Value.Length;
        return total;
    }

    [Benchmark(OperationsPerInvoke = N)]
    public int NativeScan()
    {
        using RocksDbSharp.Iterator it = _nativeFilled!.NewIterator();
        int total = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next()) total += it.GetKeySpan().Length + it.GetValueSpan().Length;
        return total;
    }
}
