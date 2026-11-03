using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.TrieIndex;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// The trie user-defined index (TrieIndexFactory) versus the standard binary-search index on the same table: table
/// Get and Seek with cached blocks, and the raw index seek (LoudsTrieIterator vs the index block iterator). Every
/// method must report "Allocated -".
/// </summary>
[MemoryDiagnoser]
public class TrieIndexBenchmarks
{
    private const int NumEntries = 200_000;
    private BlockBasedTableReader _reader = null!;
    private BlockBasedTableIterator _trieIter = null!;
    private BlockBasedTableIterator _stdIter = null!;
    private BlockIter _indexIter = null!;
    private LoudsTrieIterator _trie = null!;
    private byte[][] _lookupKeys = null!;
    private byte[][] _lookupUserKeys = null!;
    private PooledBufferWriter _value = null!;
    private int _next;

    private static byte[] UserKey(int i) => System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12"));

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(1);
        var file = new TableBenchmarks.MemWritableFile();
        var format = TableFormatOptions.Default with
        {
            BlockSize = 4096,
            UserDefinedIndexFactory = TrieIndexFactory.Instance,
            IndexMode = BlockBasedIndexMode.StandardDefault,
        };
        using (var b = new BlockBasedTableBuilder(file, format, new TableBuilderOptions()))
        {
            byte[] v = new byte[100];
            for (int i = 0; i < NumEntries; i++)
            {
                rnd.NextBytes(v);
                byte[] k = new byte[16 + 8];
                DbFormat.AppendInternalKey(k, UserKey(i), (ulong)i + 1, EntryType.Value);
                b.Add(k, v);
            }
            b.Finish();
        }
        var cache = Cache.BlockCache.CreateHyperClock(new Cache.HyperClockCacheOptions { Capacity = 256L << 20, EstimatedEntryCharge = 4096 });
        _reader = BlockBasedTableReader.Open(new TableBenchmarks.MemFile(file.ToArray()), new TableReaderOptions
        {
            BlockCache = cache, UserDefinedIndexFactory = TrieIndexFactory.Instance, IndexMode = BlockBasedIndexMode.StandardDefault,
        });
        _trieIter = _reader.NewIterator(TableReadFlags.PreferCustomIndex);
        _stdIter = _reader.NewIterator(TableReadFlags.BuiltinIndex);
        _indexIter = new BlockIter();
        _reader.InitIndexIter(_indexIter, _reader.IndexBlock, isPartition: false);
        _trie = new LoudsTrieIterator(((TrieIndexReader)_reader.UserDefinedIndex!).Trie);
        _value = new PooledBufferWriter(256);
        _lookupKeys = new byte[4096][];
        _lookupUserKeys = new byte[4096][];
        for (int i = 0; i < _lookupKeys.Length; i++)
        {
            byte[] uk = UserKey(rnd.Next(NumEntries));
            byte[] k = new byte[16 + 8];
            DbFormat.AppendInternalKey(k, uk, DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            _lookupKeys[i] = k;
            _lookupUserKeys[i] = uk;
        }
        // Warm the block cache.
        foreach (byte[] k in _lookupKeys) _reader.Get(k, null, TableReadFlags.None);
        for (_trieIter.SeekToFirst(); _trieIter.Valid; _trieIter.Next()) { }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _trieIter.Dispose();
        _stdIter.Dispose();
        _indexIter.Dispose();
        _reader.Dispose();
        _value.Dispose();
    }

    private byte[] NextKey() => _lookupKeys[_next++ & 4095];

    [Benchmark(Baseline = true, Description = "Get: standard index")]
    public bool GetStandard()
    {
        _value.Clear();
        return _reader.Get(NextKey(), _value, TableReadFlags.BuiltinIndex).Found;
    }

    [Benchmark(Description = "Get: trie index")]
    public bool GetTrie()
    {
        _value.Clear();
        return _reader.Get(NextKey(), _value, TableReadFlags.PreferCustomIndex).Found;
    }

    [Benchmark(Description = "Seek: standard index")]
    public bool SeekStandard()
    {
        _stdIter.Seek(NextKey());
        return _stdIter.Valid;
    }

    [Benchmark(Description = "Seek: trie index")]
    public bool SeekTrie()
    {
        _trieIter.Seek(NextKey());
        return _trieIter.Valid;
    }

    [Benchmark(Description = "Index only: index block Seek")]
    public bool IndexBlockSeek()
    {
        _indexIter.Seek(NextKey());
        return _indexIter.Valid;
    }

    [Benchmark(Description = "Index only: LoudsTrieIterator Seek")]
    public bool TrieSeek() => _trie.Seek(_lookupUserKeys[_next++ & 4095]);
}
