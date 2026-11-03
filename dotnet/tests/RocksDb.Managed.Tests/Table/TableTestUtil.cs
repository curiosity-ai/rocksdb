using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Table;
using RocksDb.Managed.Table.Filters;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Table;

/// <summary>In-memory append-only file.</summary>
internal sealed class MemWritableFile : IWritableFile
{
    private readonly MemoryStream _ms = new();
    public long Size => _ms.Length;
    public byte[] ToArray() => _ms.ToArray();
    public void Append(ReadOnlySpan<byte> data) => _ms.Write(data);
    public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        _ms.Write(data.Span);
        return ValueTask.CompletedTask;
    }
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
    public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
    public void Flush() { }
    public void Sync() { }
    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>In-memory random access file with a read counter.</summary>
internal sealed class MemRandomAccessFile(byte[] data) : IRandomAccessFile
{
    public int Reads;
    public long Length => data.Length;
    public int Read(long offset, Span<byte> destination)
    {
        Interlocked.Increment(ref Reads);
        if (offset >= data.Length) return 0;
        int n = (int)Math.Min(destination.Length, data.Length - offset);
        data.AsSpan((int)offset, n).CopyTo(destination);
        return n;
    }
    public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        await Task.Yield();
        return Read(offset, destination.Span);
    }
    /// <summary>When true, <see cref="Prefetch"/> succeeds (an OS readahead) and records its ranges.</summary>
    public bool SupportsPrefetch;
    public readonly List<(long Offset, long Length)> PrefetchCalls = new();
    public bool Prefetch(long offset, long length)
    {
        if (!SupportsPrefetch) return false;
        PrefetchCalls.Add((offset, length));
        return true;
    }
    public void Dispose() { }
}

/// <summary>Test-only exact-set filter policy (stream C provides the real Bloom/Ribbon policies).</summary>
internal sealed class ExactSetFilterPolicy : FilterPolicy
{
    public int Queries;
    public override string CompatibilityName => "test.ExactSetFilter";
    public override IFilterBitsBuilder CreateBuilder(int formatVersion) => new Builder();
    public override IFilterBitsReader CreateReader(ReadOnlyMemory<byte> contents) => new Reader(this, contents);

    private sealed class Builder : IFilterBitsBuilder
    {
        private readonly List<byte[]> _keys = new();
        public void AddKey(ReadOnlySpan<byte> key)
        {
            if (_keys.Count > 0 && _keys[^1].AsSpan().SequenceEqual(key)) return;
            _keys.Add(key.ToArray());
        }
        public int EstimatedEntries => _keys.Count;
        public void Finish(IBufferWriter<byte> output)
        {
            foreach (byte[] k in _keys) RocksDb.Managed.Util.Coding.PutLengthPrefixedSlice(output, k);
            _keys.Clear();
        }
        public void Dispose() { }
    }

    private sealed class Reader : IFilterBitsReader
    {
        private readonly HashSet<string> _set = new();
        private readonly ExactSetFilterPolicy _p;
        public Reader(ExactSetFilterPolicy p, ReadOnlyMemory<byte> contents)
        {
            _p = p;
            ReadOnlySpan<byte> s = contents.Span;
            while (s.Length > 0 && RocksDb.Managed.Util.Coding.GetLengthPrefixedSlice(ref s, out ReadOnlySpan<byte> k)) _set.Add(Convert.ToHexString(k));
        }
        public bool MayMatch(ReadOnlySpan<byte> key)
        {
            Interlocked.Increment(ref _p.Queries);
            return _set.Contains(Convert.ToHexString(key));
        }
    }
}

internal static class TableTestUtil
{
    public static byte[] IKey(string userKey, ulong seq, EntryType type = EntryType.Value) => IKey(System.Text.Encoding.UTF8.GetBytes(userKey), seq, type);

    public static byte[] IKey(ReadOnlySpan<byte> userKey, ulong seq, EntryType type = EntryType.Value)
    {
        byte[] k = new byte[userKey.Length + 8];
        DbFormat.AppendInternalKey(k, userKey, seq, type);
        return k;
    }

    public static byte[] LookupKey(string userKey, ulong seq = DbFormat.MaxSequenceNumber) => IKey(userKey, seq, DbFormat.ValueTypeForSeek);

    public static string UserKeyOf(ReadOnlySpan<byte> ikey) => System.Text.Encoding.UTF8.GetString(DbFormat.ExtractUserKey(ikey));

    /// <summary>Builds a table from sorted (internal key, value) pairs and returns the file bytes.</summary>
    public static byte[] BuildTable(IEnumerable<(byte[] Key, byte[] Value)> entries, TableFormatOptions options, TableBuilderOptions? tbo = null)
    {
        var file = new MemWritableFile();
        using (var b = new BlockBasedTableBuilder(file, options, tbo ?? new TableBuilderOptions()))
        {
            foreach ((byte[] k, byte[] v) in entries) b.Add(k, v);
            b.Finish();
            Assert.AreEqual((ulong)file.Size, b.FileSize);
        }
        return file.ToArray();
    }

    public static BlockBasedTableReader Open(byte[] data, TableReaderOptions? options = null) =>
        BlockBasedTableReader.Open(new MemRandomAccessFile(data), options ?? TableReaderOptions.Default);

    /// <summary>Deterministic sorted key/value set: keys "key%08d" (step 3) with seqs, random values.</summary>
    public static List<(byte[] Key, byte[] Value)> MakeEntries(int count, int seed, int maxValue = 100)
    {
        var rnd = new Random(seed);
        var list = new List<(byte[], byte[])>(count);
        for (int i = 0; i < count; i++)
        {
            byte[] v = new byte[rnd.Next(0, maxValue)];
            rnd.NextBytes(v);
            list.Add((IKey("key" + (i * 3).ToString("D8"), (ulong)(1000 + i)), v));
        }
        return list;
    }

    public static string Hex(ReadOnlySpan<byte> b) => Convert.ToHexString(b);
}
