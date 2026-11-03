using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Benchmarks.DbBench;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Table building throughput (the flush / compaction output path): one SST of <see cref="NumEntries"/> sorted entries
/// (16-byte keys, 100-byte db_bench values with compression ratio 0.5, 4 KiB blocks, format_version 6, bloom filter)
/// with serial vs parallel data block compression (CompressionOptions.parallel_threads).
/// </summary>
[MemoryDiagnoser]
public class TableBuildBenchmarks
{
    private const int NumEntries = 200_000;
    private byte[][] _keys = null!;
    private ValueBuffer _values = null!;
    private TableFormatOptions _format = null!;
    private TableBuilderOptions _tbo = null!;

    [Params(CompressionType.LZ4, CompressionType.Zstd)]
    public CompressionType Compression { get; set; }

    [Params(1u, 2u, 4u)]
    public uint ParallelThreads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = new byte[NumEntries][];
        for (int i = 0; i < NumEntries; i++)
        {
            byte[] k = new byte[16 + 8];
            DbFormat.AppendInternalKey(k, System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12", System.Globalization.CultureInfo.InvariantCulture)),
                (ulong)i + 1, EntryType.Value);
            _keys[i] = k;
        }
        _values = new ValueBuffer(301, 100, 0.5);
        _format = TableFormatOptions.Default with { FormatVersion = 6, FilterPolicy = new Table.Filters.BloomFilterPolicy(10) };
        _tbo = new TableBuilderOptions { CompressionType = Compression, CompressionOptions = new CompressionOptions { ParallelThreads = ParallelThreads } };
    }

    [Benchmark]
    public ulong BuildTable()
    {
        var file = new TableBenchmarks.MemWritableFile();
        var cursor = new ValueCursor(_values);
        using var b = new BlockBasedTableBuilder(file, _format, _tbo);
        for (int i = 0; i < _keys.Length; i++) b.Add(_keys[i], cursor.Next(100).Span);
        b.Finish();
        return b.FileSize;
    }
}
