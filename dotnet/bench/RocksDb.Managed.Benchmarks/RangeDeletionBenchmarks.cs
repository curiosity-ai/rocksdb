using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Benchmarks.DbBench;

namespace RocksDb.Managed.Benchmarks;

// Reads over keys hidden by range tombstones, managed vs native (rocksdb-sharp) on the same DB shape:
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories RangeDel
//
// 200k keys (100 B values) compacted to the last level, then DeleteRange of 99% of every 2000-key chunk (100 range
// tombstones) flushed to an L0 file, so a scan sees 2000 live keys among 200k deleted ones. Upstream skips covered
// keys inside the merging iterator (range tombstone keys in the heap, reseek to the tombstone end); a design that
// checks every point key against the tombstones pays per deleted key.

/// <summary>Scans and seeks over a DB where most keys are covered by range tombstones.</summary>
[MemoryDiagnoser]
[BenchmarkCategory("RangeDel")]
public class RangeDeletionBenchmarks
{
    private const int KeyCount = 200_000;
    private const int Chunk = 2000;
    private const int LivePerChunk = 20;
    private const int KeySize = 16;
    private const int Seeks = 1000;

    private string _dir = "";
    private RocksDatabase _db = null!;
    private Iterator _it = null!;
    private string _nativeDir = "";
    private RocksDbSharp.RocksDb _native = null!;
    private RocksDbSharp.Iterator _nativeIt = null!;
    private byte[][] _seekKeys = null!;

    [GlobalSetup]
    public void Setup()
    {
        byte[] value = new byte[100];
        new Random(5).NextBytes(value);
        byte[] key = new byte[KeySize], end = new byte[KeySize];

        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-rangedel-" + Environment.ProcessId);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
        var cfo = new ColumnFamilyOptions { DisableAutoCompactions = true };
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cfo);

        _nativeDir = _dir + "-native";
        if (Directory.Exists(_nativeDir)) Directory.Delete(_nativeDir, true);
        var nopts = new RocksDbSharp.DbOptions().SetCreateIfMissing(true).SetDisableAutoCompactions(1);
        _native = RocksDbSharp.RocksDb.Open(nopts, _nativeDir);

        for (int i = 0; i < KeyCount; i++)
        {
            KeyFormat.Make(key, (ulong)i);
            _db.Put(key, value);
            _native.Put(key, value);
        }
        _db.Flush();
        _db.CompactRange();
        _native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));
        _native.CompactRange((byte[]?)null, null);
        using (var wb = new RocksDbSharp.WriteBatch())
        {
            for (int c = 0; c < KeyCount; c += Chunk)
            {
                KeyFormat.Make(key, (ulong)(c + LivePerChunk));
                KeyFormat.Make(end, (ulong)(c + Chunk));
                _db.DeleteRange(key, end);
                wb.DeleteRange(key, (ulong)key.Length, end, (ulong)end.Length);
            }
            _native.Write(wb);
        }
        _db.Flush();
        _native.Flush(new RocksDbSharp.FlushOptions().SetWaitForFlush(true));

        _it = _db.NewIterator();
        _nativeIt = _native.NewIterator();
        var rng = new Random(9);
        _seekKeys = new byte[Seeks][];
        for (int i = 0; i < Seeks; i++)
        {
            _seekKeys[i] = new byte[KeySize];
            KeyFormat.Make(_seekKeys[i], (ulong)rng.Next(KeyCount));
        }
        // Warm the block caches.
        ManagedScan();
        NativeScan();
        ManagedReverseScan();
        NativeReverseScan();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _it.Dispose();
        _db.Dispose();
        _nativeIt.Dispose();
        _native.Dispose();
        try { Directory.Delete(_dir, true); } catch (IOException) { }
        try { Directory.Delete(_nativeDir, true); } catch (IOException) { }
    }

    /// <summary>Full forward scan (2000 live keys).</summary>
    [Benchmark]
    public long ManagedScan()
    {
        long n = 0;
        for (_it.SeekToFirst(); _it.Valid; _it.Next()) n += _it.Value.Length;
        return n;
    }

    [Benchmark]
    public long NativeScan()
    {
        long n = 0;
        for (_nativeIt.SeekToFirst(); _nativeIt.Valid(); _nativeIt.Next()) n += _nativeIt.GetValueSpan().Length;
        return n;
    }

    [Benchmark]
    public long ManagedReverseScan()
    {
        long n = 0;
        for (_it.SeekToLast(); _it.Valid; _it.Prev()) n += _it.Value.Length;
        return n;
    }

    [Benchmark]
    public long NativeReverseScan()
    {
        long n = 0;
        for (_nativeIt.SeekToLast(); _nativeIt.Valid(); _nativeIt.Prev()) n += _nativeIt.GetValueSpan().Length;
        return n;
    }

    /// <summary>1000 x (random Seek + 10 Next); most seek targets are deleted.</summary>
    [Benchmark(OperationsPerInvoke = Seeks)]
    public long ManagedSeekNext10()
    {
        long n = 0;
        foreach (byte[] k in _seekKeys)
        {
            _it.Seek(k);
            for (int j = 0; j < 10 && _it.Valid; j++, _it.Next()) n += _it.Value.Length;
        }
        return n;
    }

    [Benchmark(OperationsPerInvoke = Seeks)]
    public long NativeSeekNext10()
    {
        long n = 0;
        foreach (byte[] k in _seekKeys)
        {
            _nativeIt.Seek(k);
            for (int j = 0; j < 10 && _nativeIt.Valid(); j++, _nativeIt.Next()) n += _nativeIt.GetValueSpan().Length;
        }
        return n;
    }
}
