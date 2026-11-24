using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// The memtable read pattern of a flush: an in-order scan of a randomly filled memtable (nodes scattered over the arena)
/// that copies every key and value, as <c>BlockBasedTableBuilder.Add</c> does into the data block. 500k entries of
/// 16-byte keys and <see cref="ValueSize"/>-byte values fill about the default 64 MiB write buffer (100-byte values).
/// Results are per entry.
/// </summary>
[MemoryDiagnoser]
public class MemTableFlushScanBenchmarks
{
    private MemTable? _mem;
    private byte[] _scratch = [];
    private int _entries;

    [Params(100, 1024)]
    public int ValueSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _entries = ValueSize >= 1024 ? 60_000 : 500_000;
        var rnd = new Random(42);
        byte[] key = new byte[16];
        byte[] value = new byte[ValueSize];
        rnd.NextBytes(value);
        _mem = new MemTable(InternalKeyComparator.Bytewise, new MemTableOptions { WriteBufferSize = 1L << 30 }, 0);
        for (int i = 0; i < _entries; i++)
        {
            rnd.NextBytes(key);
            _mem.Add((ulong)i + 1, EntryType.Value, key, value);
        }
        _scratch = new byte[64 * 1024];
    }

    [GlobalCleanup]
    public void Cleanup() => _mem?.Dispose();

    /// <summary>Scan + copy of key and value (the flush's table builder input).</summary>
    [Benchmark]
    public int ScanCopy()
    {
        MemTableIterator it = _mem!.NewIterator();
        Span<byte> scratch = _scratch;
        int pos = 0;
        int n = 0;
        for (it.SeekToFirst(); it.Valid; it.Next())
        {
            ReadOnlySpan<byte> k = it.Key;
            ReadOnlySpan<byte> v = it.Value;
            if (pos + k.Length + v.Length > scratch.Length) pos = 0;
            k.CopyTo(scratch[pos..]);
            pos += k.Length;
            v.CopyTo(scratch[pos..]);
            pos += v.Length;
            n++;
        }
        return n;
    }
}
