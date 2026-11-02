using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Wbwi;

namespace RocksDb.Managed;

/// <summary>
/// Iterator over the indexed entries of one column family of a <see cref="WriteBatchWithIndex"/>: keys in comparator
/// order, every update of a key (most recent first). Port of WBWIIteratorImpl
/// (utilities/write_batch_with_index/write_batch_with_index_internal.{h,cc}), including the optional iterate bounds.
/// The spans of <see cref="Key"/> / <see cref="Value"/> point into the batch and are valid until the batch changes.
/// Invalidated by writes to the batch (re-seek after modifying it). Not thread-safe.
/// </summary>
public sealed class WriteBatchWithIndexIterator
{
    private readonly WriteBatchWithIndex _wbwi;
    private readonly WriteBatchEntryIndex _index;
    private readonly uint _cf;
    private readonly Comparator _cmp;
    private readonly ReadOnlyMemory<byte>? _lower;
    private readonly ReadOnlyMemory<byte>? _upper;
    private int _node = -1;
    private bool _outOfBound;

    internal WriteBatchWithIndexIterator(WriteBatchWithIndex wbwi, uint cf, Comparator cmp, ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
    {
        _wbwi = wbwi;
        _index = wbwi.Index;
        _cf = cf;
        _cmp = cmp;
        _lower = lower;
        _upper = upper;
    }

    private bool ValidRegardlessOfBoundLimit => _node >= 0 && _index.ColumnFamily(_node) == _cf;

    /// <summary>True if positioned at an entry of the column family within the bounds.</summary>
    public bool Valid => !_outOfBound && ValidRegardlessOfBoundLimit;

    private void TestBounds()
    {
        _outOfBound = false;
        if (!ValidRegardlessOfBoundLimit || (_lower is null && _upper is null)) return;
        ReadOnlySpan<byte> k = Key;
        _outOfBound = AtOrAfterUpperBound(k) || BeforeLowerBound(k);
    }

    private bool AtOrAfterUpperBound(ReadOnlySpan<byte> k) => _upper is { } u && _cmp.CompareWithoutTimestamp(k, false, u.Span, false) >= 0;

    private bool BeforeLowerBound(ReadOnlySpan<byte> k) => _lower is { } l && _cmp.CompareWithoutTimestamp(k, false, l.Span, false) < 0;

    public void SeekToFirst()
    {
        _node = _lower is { } l ? _index.Seek(_cf, l.Span, long.MaxValue) : _index.SeekToFirstOfCf(_cf);
        TestBounds();
    }

    public void SeekToLast()
    {
        // The entry before the first one >= the upper bound (or before the next column family).
        _node = _upper is { } u ? _index.SeekLessThan(_cf, u.Span, long.MaxValue) : _index.SeekLessThanCf(_cf + 1);
        TestBounds();
    }

    /// <summary>Positions at the first entry with key &gt;= <paramref name="key"/> (its most recent update).</summary>
    public void Seek(ReadOnlySpan<byte> key)
    {
        if (BeforeLowerBound(key))
        {
            SeekToFirst();
            return;
        }
        _node = _index.Seek(_cf, key, long.MaxValue);
        TestBounds();
    }

    /// <summary>Positions at the last entry with key &lt;= <paramref name="key"/> (its oldest update).</summary>
    public void SeekForPrev(ReadOnlySpan<byte> key)
    {
        if (AtOrAfterUpperBound(key))
        {
            SeekToLast();
            return;
        }
        // Last entry <= (key, offset 0): every entry of `key` is included.
        _node = _index.SeekLessThan(_cf, key, -1);
        TestBounds();
    }

    public void Next()
    {
        _node = _index.NextNode(_node);
        TestBounds();
    }

    public void Prev()
    {
        _node = _index.PrevNode(_node);
        TestBounds();
    }

    /// <summary>The kind of the current entry.</summary>
    public WriteType Type => _wbwi.GetEntry(_index.RecordOffset(_node), out _, out _);

    /// <summary>The key of the current entry.</summary>
    public ReadOnlySpan<byte> Key => _index.Key(_node);

    /// <summary>The value of the current entry (Put value, Merge operand or serialized entity; empty for deletes).</summary>
    public ReadOnlySpan<byte> Value
    {
        get
        {
            _wbwi.GetEntry(_index.RecordOffset(_node), out _, out ReadOnlySpan<byte> v);
            return v;
        }
    }

    internal WriteType Entry(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value) => _wbwi.GetEntry(_index.RecordOffset(_node), out key, out value);

    private bool MatchesKey(ReadOnlySpan<byte> key) => Valid && _cmp.CompareWithoutTimestamp(key, false, Key, false) == 0;

    /// <summary>WBWIIteratorImpl::AdvanceKey: moves forward/backward until the iterator is at a different key.</summary>
    private void AdvanceKey(bool forward)
    {
        if (!Valid) return;
        int len = Key.Length;
        if (_advanceKey.Length < len) _advanceKey = new byte[Math.Max(len, _advanceKey.Length * 2)];
        Key.CopyTo(_advanceKey);
        do
        {
            if (forward) Next();
            else Prev();
        } while (MatchesKey(_advanceKey.AsSpan(0, len)));
    }

    private byte[] _advanceKey = new byte[64];
    private byte[] _findKey = new byte[64];

    /// <summary>Moves to the first entry of the next key.</summary>
    public void NextKey() => AdvanceKey(true);

    /// <summary>Moves to the first entry of the previous key.</summary>
    public void PrevKey()
    {
        AdvanceKey(false); // Move to the tail of the previous key
        if (Valid)
        {
            AdvanceKey(false); // Move back another key. Now we are at the start of the previous key
            if (Valid) Next(); // Move forward one onto this key
            else SeekToFirst(); // Not valid, move to the start
        }
    }

    /// <summary>
    /// WBWIIteratorImpl::FindLatestUpdate(merge_context) for the current key: moves to its most recent Put/PutEntity/Delete
    /// (or to its oldest Merge when there are only merges), collecting merge operands newest first.
    /// </summary>
    internal WriteBatchWithIndex.LookupResult FindLatestUpdate(MergeContext context)
    {
        context.Clear();
        if (!Valid) return WriteBatchWithIndex.LookupResult.NotFound;
        int keyLen = Key.Length;
        if (_findKey.Length < keyLen) _findKey = new byte[Math.Max(keyLen, _findKey.Length * 2)];
        Key.CopyTo(_findKey);
        ReadOnlySpan<byte> key = _findKey.AsSpan(0, keyLen);
        // Move to the first entry of the current key.
        AdvanceKey(false);
        if (Valid) Next();
        else SeekToFirst();
        var result = WriteBatchWithIndex.LookupResult.NotFound;
        while (Valid)
        {
            WriteType t = Entry(out ReadOnlySpan<byte> k, out ReadOnlySpan<byte> v);
            if (_cmp.CompareWithoutTimestamp(k, false, key, false) != 0) break;
            switch (t)
            {
                case WriteType.Put:
                case WriteType.PutEntity:
                    return WriteBatchWithIndex.LookupResult.Found;
                case WriteType.Delete:
                case WriteType.SingleDelete:
                    return WriteBatchWithIndex.LookupResult.Deleted;
                case WriteType.Merge:
                    result = WriteBatchWithIndex.LookupResult.MergeInProgress;
                    context.PushOperand(v);
                    break;
            }
            Next();
        }
        // Only merges: back to the last entry of this key.
        if (Valid) Prev();
        else SeekToLast();
        return result;
    }
}
