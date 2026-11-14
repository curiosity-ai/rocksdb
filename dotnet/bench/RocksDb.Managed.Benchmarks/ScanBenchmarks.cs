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

/// <summary>
/// Short bounded scans (iterate_upper_bound through <see cref="Iterator.SetBounds"/>, one reused iterator) over a compacted
/// DB: the table iterator stops at the data block holding the upper bound instead of reading the next one
/// (BlockBasedTableIterator::FindBlockForward next_block_is_out_of_bound). <c>CacheMiB</c> 1 makes most data blocks
/// miss. "Allocated" must be "-".
/// </summary>
[MemoryDiagnoser, MinColumn, MedianColumn]
[BenchmarkCategory("Scan")]
public class BoundedScanBenchmarks
{
    private const int Keys = 200_000;
    private const int Ranges = 256;

    [Params(16, 256)]
    public int RangeRows { get; set; }

    [Params(1, 256)]
    public int CacheMiB { get; set; }

    private string _dir = "";
    private RocksDatabase _db = null!;
    private Iterator _it = null!;
    private byte[][] _lower = [];
    private byte[][] _upper = [];

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-boundedscan-" + Guid.NewGuid().ToString("N"));
        var value = new byte[100];
        new Random(3).NextBytes(value);
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, Compression = Compression.CompressionType.None };
        cf.TableOptions.BlockCache = Cache.BlockCache.CreateLru((long)CacheMiB << 20);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cf);
        for (int i = 0; i < Keys; i++) _db.Put(ScanKeys.Key(i), value);
        _db.Flush();
        _db.CompactRange();
        var rnd = new Random(5);
        _lower = new byte[Ranges][];
        _upper = new byte[Ranges][];
        for (int r = 0; r < Ranges; r++)
        {
            int start = rnd.Next(Keys - RangeRows);
            _lower[r] = ScanKeys.Key(start);
            _upper[r] = ScanKeys.Key(start + RangeRows);
        }
        _it = _db.NewIterator(new ReadOptions { FillCache = CacheMiB > 1 });
        Scan();
    }

    /// <summary>One operation = <see cref="Ranges"/> bounded scans.</summary>
    [Benchmark]
    public long Scan()
    {
        long n = 0;
        for (int r = 0; r < Ranges; r++)
        {
            _it.SetBounds(_lower[r], _upper[r]);
            for (_it.SeekToFirst(); _it.Valid; _it.Next()) n += _it.Value.Length;
        }
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
/// Long bounded scans with an explicit 16 MiB readahead (Mosaik's RocksDbDataStorage scans: iterate bounds +
/// readahead_size) over a compacted DB whose data mostly misses a 1 MiB block cache. With
/// <see cref="ReadOptions.AutoReadaheadSize"/> the readahead buffer is trimmed to the blocks below the upper bound
/// (BlockCacheLookupForReadAheadSize); without it every scan reads its 16 MiB window. <c>FreshIterator</c>: a new
/// iterator per scan (Mosaik), else one iterator re-bounded with <see cref="Iterator.SetBounds"/> ("Allocated" must be "-").
/// </summary>
[MemoryDiagnoser, MinColumn, MedianColumn]
[BenchmarkCategory("Scan")]
public class ReadaheadBoundedScanBenchmarks
{
    private const int Keys = 200_000;
    private const int Ranges = 32;
    private const int RangeRows = 1000;

    [Params(false, true)]
    public bool AutoReadaheadSize { get; set; }

    [Params(false, true)]
    public bool FreshIterator { get; set; }

    private string _dir = "";
    private RocksDatabase _db = null!;
    private Iterator _it = null!;
    private ReadOptions[] _options = [];
    private byte[][] _lower = [];
    private byte[][] _upper = [];

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-rascan-" + Guid.NewGuid().ToString("N"));
        var value = new byte[100];
        new Random(3).NextBytes(value);
        var cf = new ColumnFamilyOptions { DisableAutoCompactions = true, Compression = Compression.CompressionType.None };
        cf.TableOptions.BlockCache = Cache.BlockCache.CreateLru(1 << 20);
        _db = RocksDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, cf);
        for (int i = 0; i < Keys; i++) _db.Put(ScanKeys.Key(i), value);
        _db.Flush();
        _db.CompactRange();
        var rnd = new Random(5);
        _lower = new byte[Ranges][];
        _upper = new byte[Ranges][];
        _options = new ReadOptions[Ranges];
        for (int r = 0; r < Ranges; r++)
        {
            int start = rnd.Next(Keys - RangeRows);
            _lower[r] = ScanKeys.Key(start);
            _upper[r] = ScanKeys.Key(start + RangeRows);
            _options[r] = new ReadOptions
            {
                IterateLowerBound = _lower[r], IterateUpperBound = _upper[r], ReadaheadSize = 16UL << 20, AutoReadaheadSize = AutoReadaheadSize,
            };
        }
        _it = _db.NewIterator(new ReadOptions { ReadaheadSize = 16UL << 20, AutoReadaheadSize = AutoReadaheadSize });
        Scan();
    }

    /// <summary>One operation = <see cref="Ranges"/> bounded scans of <see cref="RangeRows"/> rows.</summary>
    [Benchmark]
    public long Scan()
    {
        long n = 0;
        for (int r = 0; r < Ranges; r++)
        {
            if (FreshIterator)
            {
                using Iterator it = _db.NewIterator(_options[r]);
                for (it.Seek(_lower[r]); it.Valid; it.Next()) n += it.Value.Length;
            }
            else
            {
                _it.SetBounds(_lower[r], _upper[r]);
                for (_it.Seek(_lower[r]); _it.Valid; _it.Next()) n += _it.Value.Length;
            }
        }
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
