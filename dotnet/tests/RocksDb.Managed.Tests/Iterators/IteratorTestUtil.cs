using System.Buffers;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Tests.Table;
using EntryType = RocksDb.Managed.Db.EntryType;

namespace RocksDb.Managed.Tests.Iterators;

/// <summary>
/// Vector-backed internal iterator. Port of the TestIterator harness of db/db_iter_test.cc (and VectorIterator,
/// util/vector_iterator.h): entries are added with increasing sequence numbers (or explicit ones), then sorted by
/// <see cref="Finish"/>.
/// </summary>
internal sealed class TestInternalIterator : InternalIterator
{
    private readonly InternalKeyComparator _icmp;
    private readonly List<(byte[] Key, byte[] Value)> _data = new();
    private ulong _nextSeq;
    private int _pos = -1;

    public TestInternalIterator(InternalKeyComparator icmp) => _icmp = icmp;
    public TestInternalIterator() : this(InternalKeyComparator.Bytewise) { }

    public int Steps; // positioning calls, for reseek assertions

    public void AddPut(string key, string value) => Add(key, EntryType.Value, value);
    public void AddDeletion(string key) => Add(key, EntryType.Deletion, "");
    public void AddSingleDeletion(string key) => Add(key, EntryType.SingleDeletion, "");
    public void AddMerge(string key, string value) => Add(key, EntryType.Merge, value);
    public void Add(string key, EntryType type, string value) => Add(key, type, value, _nextSeq++);

    public void Add(string key, EntryType type, string value, ulong seq) =>
        Add(Encoding.UTF8.GetBytes(key), type, Encoding.UTF8.GetBytes(value), seq);

    public void Add(byte[] key, EntryType type, byte[] value, ulong seq) =>
        _data.Add((TableTestUtil.IKey(key, seq, type), value));

    public void Finish() => _data.Sort((a, b) => _icmp.Compare(a.Key, b.Key));

    public override bool Valid => _pos >= 0 && _pos < _data.Count;
    public override ReadOnlySpan<byte> Key => _data[_pos].Key;
    public override ReadOnlySpan<byte> Value => _data[_pos].Value;

    public override void SeekToFirst()
    {
        Steps++;
        _pos = 0;
    }

    public override void SeekToLast()
    {
        Steps++;
        _pos = _data.Count - 1;
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        Steps++;
        _pos = LowerBound(target);
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        Steps++;
        int i = LowerBound(target);
        if (i < _data.Count && _icmp.Compare(_data[i].Key, target) == 0) _pos = i;
        else _pos = i - 1;
    }

    private int LowerBound(ReadOnlySpan<byte> target)
    {
        int lo = 0, hi = _data.Count;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_icmp.Compare(_data[mid].Key, target) < 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    public override void Next()
    {
        Assert.IsTrue(Valid);
        Steps++;
        _pos++;
    }

    public override void Prev()
    {
        Assert.IsTrue(Valid);
        Steps++;
        _pos--;
        if (_pos < 0) _pos = _data.Count; // invalid
    }
}

/// <summary>Test operator equivalent to StringAppendTESTOperator: ',' join, no partial merge.</summary>
internal sealed class StringAppendNoPartialOperator : MergeOperator
{
    private readonly StringAppendOperator _inner = new();
    public override string Name => "StringAppendTESTOperator";
    public override bool FullMerge(ReadOnlySpan<byte> key, bool hasExistingValue, ReadOnlySpan<byte> existingValue, in MergeOperands operands, IBufferWriter<byte> result) =>
        _inner.FullMerge(key, hasExistingValue, existingValue, operands, result);
}

/// <summary>In-memory <see cref="ITableCache"/>: table bytes by file number, readers kept open.</summary>
internal sealed class TestTableCache : ITableCache, IDisposable
{
    private readonly Dictionary<ulong, byte[]> _files = new();
    private readonly Dictionary<ulong, BlockBasedTableReader> _readers = new();
    private readonly TableReaderOptions _options;
    public int Opens;
    public int IteratorsCreated;
    public int OpenIterators;

    public TestTableCache(TableReaderOptions options) => _options = options;
    public TestTableCache() : this(TableReaderOptions.Default) { }

    public void AddFile(ulong number, byte[] data) => _files[number] = data;

    private BlockBasedTableReader Open(FileMetaData file)
    {
        lock (_readers)
        {
            if (!_readers.TryGetValue(file.FileNumber, out BlockBasedTableReader? r))
            {
                Opens++;
                r = BlockBasedTableReader.Open(new MemRandomAccessFile(_files[file.FileNumber]), _options);
                _readers[file.FileNumber] = r;
            }
            return r;
        }
    }

    public ValueTask<TableReaderLease> GetReaderAsync(FileMetaData file, CancellationToken cancellationToken = default) => new(GetReader(file));
    public TableReaderLease GetReader(FileMetaData file) => new(Open(file), null, null);

    public InternalIterator NewIterator(FileMetaData file)
    {
        IteratorsCreated++;
        OpenIterators++;
        return new TableInternalIterator(new TableReaderLease(Open(file), static s => ((TestTableCache)s!).OpenIterators--, this));
    }

    public void Evict(ulong fileNumber) { }

    public void Dispose()
    {
        foreach (BlockBasedTableReader r in _readers.Values) r.Dispose();
        _readers.Clear();
    }
}

/// <summary>One operation of a randomized history. For range deletions <see cref="Key"/> is the start, <see cref="Value"/> the end.</summary>
internal readonly record struct TestOp(string Key, ulong Seq, EntryType Type, string Value);

/// <summary>Brute-force model of what a DB iterator must return.</summary>
internal static class ReferenceModel
{
    public static List<(string Key, string Value)> Expected(IReadOnlyList<TestOp> ops, ulong snapshot, MergeOperator? mergeOperator,
        string? lower, string? upper)
    {
        var tombstones = new List<TestOp>();
        var byKey = new SortedDictionary<string, List<TestOp>>(StringComparer.Ordinal);
        foreach (TestOp op in ops)
        {
            if (op.Seq > snapshot) continue;
            if (op.Type == EntryType.RangeDeletion)
            {
                tombstones.Add(op);
                continue;
            }
            if (!byKey.TryGetValue(op.Key, out List<TestOp>? l)) byKey[op.Key] = l = new List<TestOp>();
            l.Add(op);
        }
        var result = new List<(string, string)>();
        foreach ((string key, List<TestOp> entries) in byKey)
        {
            if (lower is not null && string.CompareOrdinal(key, lower) < 0) continue;
            if (upper is not null && string.CompareOrdinal(key, upper) >= 0) continue;
            entries.Sort((a, b) => b.Seq.CompareTo(a.Seq));
            var operands = new List<string>(); // newest first
            string? baseValue = null;
            bool deleted = false;
            foreach (TestOp e in entries)
            {
                bool covered = false;
                foreach (TestOp t in tombstones)
                {
                    if (string.CompareOrdinal(t.Key, key) <= 0 && string.CompareOrdinal(key, t.Value) < 0 && t.Seq > e.Seq) covered = true;
                }
                if (covered || e.Type is EntryType.Deletion or EntryType.SingleDeletion)
                {
                    deleted = true;
                    break;
                }
                if (e.Type == EntryType.Value)
                {
                    baseValue = e.Value;
                    break;
                }
                operands.Add(e.Value);
            }
            if (operands.Count == 0)
            {
                if (baseValue is not null) result.Add((key, baseValue));
                continue;
            }
            _ = deleted;
            result.Add((key, Merge(mergeOperator!, key, baseValue, operands)));
        }
        return result;
    }

    private static string Merge(MergeOperator op, string key, string? baseValue, List<string> operandsNewestFirst)
    {
        var buf = new MergeOperandBuffer();
        foreach (string o in operandsNewestFirst) buf.PushOlder(Encoding.UTF8.GetBytes(o));
        var w = new ArrayBufferWriter<byte>();
        MergeHelper.TimedFullMerge(op, Encoding.UTF8.GetBytes(key), baseValue is not null, baseValue is null ? default : Encoding.UTF8.GetBytes(baseValue), buf.AsOperands(), w);
        buf.Dispose();
        return Encoding.UTF8.GetString(w.WrittenSpan);
    }
}

internal static class IterTestHelpers
{
    public static byte[] B(string s) => Encoding.UTF8.GetBytes(s);
    public static string S(ReadOnlySpan<byte> b) => Encoding.UTF8.GetString(b);

    public static DbIter NewDbIter(InternalIterator inner, ulong sequence, MergeOperator? mergeOperator, ReadOptions? ro, ulong maxSkip = 8, RangeDelAggregator? rangeDel = null) =>
        new(InternalKeyComparator.Bytewise, inner, sequence, ro, mergeOperator, rangeDel, maxSkip, null);

    public static List<(string, string)> ScanForward(DbIter it)
    {
        var l = new List<(string, string)>();
        for (it.SeekToFirst(); it.Valid; it.Next()) l.Add((S(it.Key), S(it.Value)));
        return l;
    }

    public static List<(string, string)> ScanReverse(DbIter it)
    {
        var l = new List<(string, string)>();
        for (it.SeekToLast(); it.Valid; it.Prev()) l.Add((S(it.Key), S(it.Value)));
        l.Reverse();
        return l;
    }

    public static string Format(List<(string Key, string Value)> l) => string.Join(" ", l.ConvertAll(static kv => kv.Key + "=" + kv.Value));
}
