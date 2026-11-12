using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Db;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>Where the index, the full filter and the compression dictionary of the table live.</summary>
public enum MetadataPlacement
{
    /// <summary>cache_index_and_filter_blocks = false: the reader owns them.</summary>
    Owned,
    /// <summary>cache_index_and_filter_blocks = true, default pinning (none): each is looked up in the block cache per use.</summary>
    Cached,
    /// <summary>cache_index_and_filter_blocks = true with unpartitioned_pinning = kAll: in the block cache, pinned by the reader.</summary>
    CachedPinned,
}

/// <summary>
/// Point lookups and a short scan on a ZSTD table compressed with a dictionary, with a bloom filter, by where the
/// metadata blocks (index, filter, compression dictionary) live. All blocks are in the block cache, so this measures
/// the block cache lookups of the metadata blocks, not decompression. Hot paths must not allocate.
/// </summary>
[MemoryDiagnoser]
public class TableMetadataCacheBenchmarks
{
    private const int NumEntries = 100_000;

    [Params(MetadataPlacement.Owned, MetadataPlacement.Cached, MetadataPlacement.CachedPinned)]
    public MetadataPlacement Placement { get; set; }

    private byte[][] _lookupKeys = null!;
    private BlockBasedTableReader _reader = null!;
    private BlockBasedTableIterator _iter = null!;
    private PooledBufferWriter _value = null!;
    private int _next;

    private static byte[] UserKey(int i) => System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12"));

    private static byte[] Value(int i) => System.Text.Encoding.ASCII.GetBytes(
        "{\"id\":" + i + ",\"name\":\"customer-" + (i % 97) + "\",\"email\":\"user" + (i % 89) + "@example.com\",\"country\":\""
        + (i % 3 == 0 ? "Portugal" : i % 3 == 1 ? "Germany" : "Brazil") + "\",\"active\":" + (i % 2 == 0 ? "true" : "false") + "}");

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(1);
        var policy = new BloomFilterPolicy(10);
        var file = new TableBenchmarks.MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, TableFormatOptions.Default with { BlockSize = 4096, FilterPolicy = policy },
            new TableBuilderOptions { CompressionType = CompressionType.Zstd, CompressionOptions = new CompressionOptions { MaxDictBytes = 16384 } }))
        {
            for (int i = 0; i < NumEntries; i++)
            {
                byte[] k = new byte[16 + 8];
                DbFormat.AppendInternalKey(k, UserKey(i), (ulong)i + 1, EntryType.Value);
                b.Add(k, Value(i));
            }
            b.Finish();
        }
        var cache = Cache.BlockCache.CreateHyperClock(new Cache.HyperClockCacheOptions { Capacity = 256L << 20, EstimatedEntryCharge = 4096 });
        _reader = BlockBasedTableReader.Open(new TableBenchmarks.MemFile(file.ToArray()), new TableReaderOptions
        {
            BlockCache = cache,
            FilterPolicy = policy,
            CacheIndexAndFilterBlocks = Placement != MetadataPlacement.Owned,
            UnpartitionedPinning = Placement == MetadataPlacement.CachedPinned ? PinningTier.All : PinningTier.Fallback,
        });
        if (_reader.CompressionDictionary.IsEmpty) throw new InvalidOperationException("the table has no compression dictionary");
        _iter = _reader.NewIterator();
        _value = new PooledBufferWriter(256);
        _lookupKeys = new byte[1024][];
        for (int i = 0; i < _lookupKeys.Length; i++)
        {
            byte[] k = new byte[16 + 8];
            DbFormat.AppendInternalKey(k, UserKey(rnd.Next(NumEntries)), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            _lookupKeys[i] = k;
        }
        // Warm the cache with every data block.
        for (_iter.SeekToFirst(); _iter.Valid; _iter.Next()) { }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _iter.Dispose();
        _reader.Dispose();
        _value.Dispose();
    }

    [Benchmark(Description = "Dict table: Get (cached blocks)")]
    public bool Get()
    {
        _value.Clear();
        return _reader.Get(_lookupKeys[_next++ & 1023], _value).Found;
    }

    [Benchmark(Description = "Dict table: Seek + 100 x Next (cached)")]
    public int SeekScan()
    {
        int n = 0;
        _iter.Seek(_lookupKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && _iter.Valid; i++, _iter.Next()) n += _iter.Value.Length;
        return n;
    }
}
