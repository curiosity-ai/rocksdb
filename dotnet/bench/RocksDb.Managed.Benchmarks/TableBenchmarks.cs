using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Benchmarks;

/// <summary>Block-level iteration/seek and table-level point lookups / scans (stream B). Hot paths must not allocate.</summary>
[MemoryDiagnoser]
public class TableBenchmarks
{
    private const int NumEntries = 100_000;
    private byte[][] _lookupKeys = null!;
    private Block _block = null!;
    private BlockIter _blockIter = null!;
    private byte[][] _blockKeys = null!;
    private BlockBasedTableReader _cachedReader = null!;
    private BlockBasedTableReader _uncachedReader = null!;
    private BlockBasedTableIterator _tableIter = null!;
    private PooledBufferWriter _value = null!;
    private int _next;

    private static byte[] UserKey(int i) => System.Text.Encoding.ASCII.GetBytes("user" + i.ToString("D12"));

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(1);
        var file = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, TableFormatOptions.Default, new TableBuilderOptions()))
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
        byte[] data = file.ToArray();
        _cachedReader = BlockBasedTableReader.Open(new MemFile(data), new TableReaderOptions { BlockCache = new SimpleBlockCache() });
        _uncachedReader = BlockBasedTableReader.Open(new MemFile(data), TableReaderOptions.Default);
        _tableIter = _cachedReader.NewIterator();
        _value = new PooledBufferWriter(256);

        _lookupKeys = new byte[1024][];
        for (int i = 0; i < _lookupKeys.Length; i++)
        {
            byte[] k = new byte[16 + 8];
            DbFormat.AppendInternalKey(k, UserKey(rnd.Next(NumEntries)), DbFormat.MaxSequenceNumber, DbFormat.ValueTypeForSeek);
            _lookupKeys[i] = k;
        }

        // A single 4 KiB-ish data block with restart interval 16.
        using var bb = new BlockBuilder(16, true, false, DataBlockIndexType.BinarySearch, 0.75);
        var keys = new List<byte[]>();
        for (int i = 0; bb.CurrentSizeEstimate < 4096; i++)
        {
            byte[] k = new byte[16 + 8];
            DbFormat.AppendInternalKey(k, UserKey(i * 2), 1, EntryType.Value);
            bb.Add(k, new byte[64]);
            keys.Add(k);
        }
        _block = Block.CreateCopy(bb.Finish());
        _blockKeys = keys.ToArray();
        _blockIter = new BlockIter();
        _blockIter.InitializeData(_block, InternalKeyComparator.Bytewise, BlockIter.DisableGlobalSequenceNumber);
        // Warm the cache.
        for (int i = 0; i < NumEntries; i += 10) _cachedReader.Get(_lookupKeys[i % _lookupKeys.Length], null);
        for (_tableIter.SeekToFirst(); _tableIter.Valid; _tableIter.Next()) { }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _tableIter.Dispose();
        _cachedReader.Dispose();
        _uncachedReader.Dispose();
        _blockIter.Dispose();
        _block.Release();
        _value.Dispose();
    }

    [Benchmark(Description = "Block: full scan (~50 entries)")]
    public int BlockScan()
    {
        int n = 0;
        for (_blockIter.SeekToFirst(); _blockIter.Valid; _blockIter.Next()) n += _blockIter.Value.Length;
        return n;
    }

    [Benchmark(Description = "Block: Seek")]
    public bool BlockSeek()
    {
        _blockIter.Seek(_blockKeys[(_next++ * 7) % _blockKeys.Length]);
        return _blockIter.Valid;
    }

    [Benchmark(Description = "Table: Get (cached blocks)")]
    public bool TableGetCached()
    {
        _value.Clear();
        return _cachedReader.Get(_lookupKeys[_next++ & 1023], _value).Found;
    }

    [Benchmark(Description = "Table: Get (block read+checksum per op)")]
    public bool TableGetUncached()
    {
        _value.Clear();
        return _uncachedReader.Get(_lookupKeys[_next++ & 1023], _value).Found;
    }

    [Benchmark(Description = "Table: Seek + 100 x Next (cached)")]
    public int TableSeekScan()
    {
        int n = 0;
        _tableIter.Seek(_lookupKeys[_next++ & 1023]);
        for (int i = 0; i < 100 && _tableIter.Valid; i++, _tableIter.Next()) n += _tableIter.Value.Length;
        return n;
    }

    private sealed class MemFile(byte[] data) : IRandomAccessFile
    {
        public long Length => data.Length;
        public int Read(long offset, Span<byte> destination)
        {
            int n = (int)Math.Min(destination.Length, data.Length - offset);
            data.AsSpan((int)offset, n).CopyTo(destination);
            return n;
        }
        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) => new(Read(offset, destination.Span));
        public void Dispose() { }
    }

    private sealed class MemWritableFile : IWritableFile
    {
        private readonly MemoryStream _ms = new();
        public long Size => _ms.Length;
        public byte[] ToArray() => _ms.ToArray();
        public void Append(ReadOnlySpan<byte> data) => _ms.Write(data);
        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) { _ms.Write(data.Span); return ValueTask.CompletedTask; }
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public void Flush() { }
        public void Sync() { }
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Minimal lock-protected cache standing in for stream F's block cache.</summary>
    private sealed class SimpleBlockCache : IBlockCache
    {
        private readonly Dictionary<BlockCacheKey, Block> _map = new();
        private readonly Lock _lock = new();
        public bool TryGet(in BlockCacheKey key, [NotNullWhen(true)] out Block? block)
        {
            lock (_lock)
            {
                if (_map.TryGetValue(key, out block)) { block.AddRef(); return true; }
            }
            return false;
        }
        public void Insert(in BlockCacheKey key, Block block)
        {
            lock (_lock) { if (_map.TryAdd(key, block)) block.AddRef(); }
        }
    }
}
