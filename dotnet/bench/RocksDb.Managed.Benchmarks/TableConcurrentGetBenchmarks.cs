using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Cached point lookups from 4 threads on one reader (all blocks resident): how the block cache scales with concurrent
/// readers. "dictionary" is a lock-protected Dictionary through the legacy IBlockCache seam (the stand-in the table
/// benchmarks used before the block cache was wired). Results are per Get.
/// </summary>
[MemoryDiagnoser]
public class TableConcurrentGetBenchmarks
{
    private const int NumEntries = 100_000;
    private const int Threads = 4;
    private const int OpsPerThread = 20_000;
    private BlockBasedTableReader _reader = null!;
    private byte[][] _lookupKeys = null!;

    [Params("hyperclock", "lru", "dictionary")]
    public string CacheType { get; set; } = "hyperclock";

    private static byte[] Key(int i, ulong seq, EntryType t)
    {
        byte[] k = new byte[16 + 8];
        DbFormat.AppendInternalKey(k, System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12")), seq, t);
        return k;
    }

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(1);
        var file = new TableBenchmarks.MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, TableFormatOptions.Default, new TableBuilderOptions()))
        {
            byte[] v = new byte[100];
            for (int i = 0; i < NumEntries; i++)
            {
                rnd.NextBytes(v);
                b.Add(Key(i, (ulong)i + 1, EntryType.Value), v);
            }
            b.Finish();
        }
        IBlockCache cache = CacheType switch
        {
            "hyperclock" => Cache.BlockCache.CreateDefault(64L << 20, 4096),
            "lru" => Cache.BlockCache.CreateLru(64L << 20),
            _ => new TableBenchmarks.SimpleBlockCache(),
        };
        _reader = BlockBasedTableReader.Open(new TableBenchmarks.MemFile(file.ToArray()), new TableReaderOptions { BlockCache = cache });
        _lookupKeys = new byte[4096][];
        for (int i = 0; i < _lookupKeys.Length; i++) _lookupKeys[i] = Key(rnd.Next(NumEntries), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
        for (int i = 0; i < NumEntries; i += 5) _reader.Get(Key(i, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek), null);
    }

    [GlobalCleanup]
    public void Cleanup() => _reader.Dispose();

    [Benchmark(OperationsPerInvoke = Threads * OpsPerThread)]
    public void Get4Threads()
    {
        Parallel.For(0, Threads, new ParallelOptions { MaxDegreeOfParallelism = Threads }, t =>
        {
            byte[][] keys = _lookupKeys;
            int idx = t * 1021;
            for (int i = 0; i < OpsPerThread; i++) _reader.Get(keys[(idx + i) & 4095], null);
        });
    }
}
