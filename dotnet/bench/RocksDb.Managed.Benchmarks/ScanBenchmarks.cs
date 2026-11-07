using BenchmarkDotNet.Attributes;

namespace RocksDb.Managed.Benchmarks;

// Full forward scans through the public iterator of a real (on-disk) DB. One operation = one full scan; "Allocated"
// must be "-" for both.
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories Scan

/// <summary>
/// Every key has 5 merge operands spread over 4 L0 files + the memtable: the forward merge references the operands in the
/// pinned blocks / memtable instead of copying them (DBIter::MergeValuesNewToOld with TempPinData).
/// </summary>
[MemoryDiagnoser]
[BenchmarkCategory("Scan")]
public class MergeScanBenchmarks
{
    private const int Keys = 20_000;

    [Params(24, 1024)]
    public int OperandSize { get; set; }

    private string _dir = "";
    private RocksDatabase _db = null!;
    private Iterator _it = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-mergescan-" + Guid.NewGuid().ToString("N"));
        var value = new byte[OperandSize];
        new Random(3).NextBytes(value);
        var cf = new ColumnFamilyOptions
        {
            MergeOperator = MergeOperator.StringAppend(","),
            DisableAutoCompactions = true,
            WriteBufferSize = 512UL << 20,
        };
        cf.TableOptions.BlockCache = Cache.BlockCache.CreateLru(1L << 30);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cf);
        for (int round = 0; round < 5; round++)
        {
            for (int i = 0; i < Keys; i++) _db.Merge(ScanKeys.Key(i), value);
            if (round < 4) _db.Flush();
        }
        _it = _db.NewIterator();
        Scan();
    }

    [Benchmark]
    public long Scan()
    {
        long n = 0;
        for (_it.SeekToFirst(); _it.Valid; _it.Next()) n += _it.Value.Length;
        return n;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _it.Dispose();
        _db.Dispose();
        ScanKeys.DeleteDirectory(_dir);
    }
}

/// <summary>
/// A 1 MiB block cache (most data blocks miss) with the implicit auto readahead of the user iterator (BlockPrefetcher +
/// AsyncPrefetchBuffer): the prefetches must not allocate (no Task per prefetch).
/// </summary>
[MemoryDiagnoser]
[BenchmarkCategory("Scan")]
public class ColdScanBenchmarks
{
    private const int Keys = 200_000;

    private string _dir = "";
    private RocksDatabase _db = null!;
    private Iterator _it = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-coldscan-" + Guid.NewGuid().ToString("N"));
        var value = new byte[100];
        new Random(3).NextBytes(value);
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, Compression = Compression.CompressionType.None };
        cf.TableOptions.BlockCache = Cache.BlockCache.CreateLru(1 << 20);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cf);
        for (int i = 0; i < Keys; i++) _db.Put(ScanKeys.Key(i), value);
        _db.Flush();
        _db.CompactRange();
        _it = _db.NewIterator(new ReadOptions { FillCache = false });
        Scan();
    }

    [Benchmark]
    public long Scan()
    {
        long n = 0;
        for (_it.SeekToFirst(); _it.Valid; _it.Next()) n += _it.Value.Length;
        return n;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _it.Dispose();
        _db.Dispose();
        ScanKeys.DeleteDirectory(_dir);
    }
}

internal static class ScanKeys
{
    public static byte[] Key(int i) => System.Text.Encoding.ASCII.GetBytes("key" + i.ToString("D10", System.Globalization.CultureInfo.InvariantCulture));

    public static void DeleteDirectory(string dir)
    {
        try { Directory.Delete(dir, recursive: true); }
        catch (IOException) { }
    }
}
