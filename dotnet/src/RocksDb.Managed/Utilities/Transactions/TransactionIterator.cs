using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Iterator over a transaction's view of a column family: the DB iterator merged with the transaction's writes.
/// Port of BaseDeltaIterator (utilities/write_batch_with_index/write_batch_with_index_internal.cc): on equal keys the
/// transaction's entry wins; deleted keys are skipped; merges in the transaction are applied on top of the DB value.
/// The transaction's iterate bounds (<see cref="ReadOptions.IterateLowerBound"/> / upper bound) apply to both sides.
/// Writes made through the transaction while the iterator is open become visible at the next positioning call.
/// Not thread-safe.
/// </summary>
public sealed class TransactionIterator : IDisposable
{
    private readonly Iterator _base;
    private readonly IndexedWriteBatch _batch;
    private readonly ColumnFamilyData _cfd;
    private readonly Comparator _ucmp;
    private readonly ReadOnlyMemory<byte>? _lower;
    private readonly ReadOnlyMemory<byte>? _upper;
    private int[] _sorted;
    private int _sortedVersion;
    private int _dpos;               // position in _sorted
    private bool _forward = true;
    private bool _valid;
    private bool _currentIsDelta;
    private bool _equalKeys;         // delta and base are at the same key (base is shadowed)
    private readonly PooledBufferWriter _value = new(64);
    private byte[] _keyScratch = new byte[64];

    internal TransactionIterator(Iterator baseIterator, IndexedWriteBatch batch, ColumnFamilyData cfd, ReadOptions? options)
    {
        _base = baseIterator;
        _batch = batch;
        _cfd = cfd;
        _ucmp = cfd.UserComparator;
        _lower = options?.IterateLowerBound;
        _upper = options?.IterateUpperBound;
        _sorted = batch.SortedEntries(cfd.Id, _ucmp);
        _sortedVersion = batch.IndexVersion;
    }

    internal TransactionIterator(Iterator baseIterator, IndexedWriteBatch batch, ColumnFamilyData cfd) : this(baseIterator, batch, cfd, null) { }

    public bool Valid => _valid;

    public ReadOnlySpan<byte> Key
    {
        get
        {
            if (!_valid) ThrowHelper.InvalidArgument("iterator is not valid");
            return _currentIsDelta ? _batch.EntryKey(_sorted[_dpos]) : _base.Key;
        }
    }

    public ReadOnlySpan<byte> Value
    {
        get
        {
            if (!_valid) ThrowHelper.InvalidArgument("iterator is not valid");
            return _currentIsDelta ? _value.WrittenSpan : _base.Value;
        }
    }

    public void SeekToFirst()
    {
        _forward = true;
        Refresh();
        if (_lower is { } lo) _base.Seek(lo.Span);
        else _base.SeekToFirst();
        _dpos = _lower is { } l ? LowerBoundIndex(l.Span, inclusive: true) : 0;
        UpdateCurrent();
    }

    public void SeekToLast()
    {
        _forward = false;
        Refresh();
        if (_upper is { } up)
        {
            _base.SeekForPrev(up.Span);
            if (_base.Valid && _ucmp.Compare(_base.Key, up.Span) >= 0) _base.Prev();
            _dpos = LowerBoundIndex(up.Span, inclusive: true) - 1;
        }
        else
        {
            _base.SeekToLast();
            _dpos = _sorted.Length - 1;
        }
        UpdateCurrent();
    }

    public void Seek(ReadOnlySpan<byte> target)
    {
        _forward = true;
        Refresh();
        _base.Seek(target);
        _dpos = LowerBoundIndex(target, inclusive: true);
        UpdateCurrent();
    }

    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        _forward = false;
        Refresh();
        _base.SeekForPrev(target);
        _dpos = LowerBoundIndex(target, inclusive: false) - 1;
        UpdateCurrent();
    }

    public void Next()
    {
        if (!_valid) ThrowHelper.InvalidArgument("iterator is not valid");
        if (!_forward || BatchChanged())
        {
            // Change direction (or re-sync with new writes): position both sides strictly after the current key.
            ReadOnlySpan<byte> k = SaveKey();
            _forward = true;
            Refresh();
            _base.Seek(k);
            if (_base.Valid && _ucmp.Compare(_base.Key, k) == 0) _base.Next();
            _dpos = LowerBoundIndex(k, inclusive: false);
        }
        else
        {
            Advance();
        }
        UpdateCurrent();
    }

    public void Prev()
    {
        if (!_valid) ThrowHelper.InvalidArgument("iterator is not valid");
        if (_forward || BatchChanged())
        {
            ReadOnlySpan<byte> k = SaveKey();
            _forward = false;
            Refresh();
            _base.SeekForPrev(k);
            if (_base.Valid && _ucmp.Compare(_base.Key, k) == 0) _base.Prev();
            _dpos = LowerBoundIndex(k, inclusive: true) - 1;
        }
        else
        {
            Advance();
        }
        UpdateCurrent();
    }

    private void Advance()
    {
        if (_currentIsDelta)
        {
            AdvanceDelta();
            if (_equalKeys) AdvanceBase();
        }
        else
        {
            AdvanceBase();
        }
    }

    private void AdvanceDelta() => _dpos += _forward ? 1 : -1;

    private void AdvanceBase()
    {
        if (_forward) _base.Next();
        else _base.Prev();
    }

    private ReadOnlySpan<byte> SaveKey()
    {
        ReadOnlySpan<byte> k = Key;
        if (_keyScratch.Length < k.Length) _keyScratch = new byte[Math.Max(k.Length, _keyScratch.Length * 2)];
        k.CopyTo(_keyScratch);
        return _keyScratch.AsSpan(0, k.Length);
    }

    private bool BatchChanged() => _batch.IndexVersion != _sortedVersion;

    private void Refresh()
    {
        if (!BatchChanged()) return;
        _sorted = _batch.SortedEntries(_cfd.Id, _ucmp);
        _sortedVersion = _batch.IndexVersion;
    }

    /// <summary>First sorted index whose key is &gt;= (inclusive) or &gt; (exclusive) <paramref name="target"/>.</summary>
    private int LowerBoundIndex(ReadOnlySpan<byte> target, bool inclusive)
    {
        int lo = 0, hi = _sorted.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            int c = _ucmp.Compare(_batch.EntryKey(_sorted[mid]), target);
            if (c < 0 || (!inclusive && c == 0)) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private bool DeltaValid()
    {
        if (_dpos < 0 || _dpos >= _sorted.Length) return false;
        ReadOnlySpan<byte> k = _batch.EntryKey(_sorted[_dpos]);
        if (_forward)
        {
            if (_upper is { } up && _ucmp.Compare(k, up.Span) >= 0) return false;
        }
        else
        {
            if (_lower is { } lo && _ucmp.Compare(k, lo.Span) < 0) return false;
        }
        return true;
    }

    /// <summary>BaseDeltaIterator::UpdateCurrent: picks the side to expose, skipping deleted keys.</summary>
    private void UpdateCurrent()
    {
        while (true)
        {
            bool deltaValid = DeltaValid();
            bool baseValid = _base.Valid;
            _equalKeys = false;
            if (!deltaValid)
            {
                _currentIsDelta = false;
                _valid = baseValid;
                return;
            }
            int idx = _sorted[_dpos];
            if (baseValid)
            {
                int cmp = _ucmp.Compare(_batch.EntryKey(idx), _base.Key);
                if (!_forward) cmp = -cmp;
                if (cmp > 0)
                {
                    // base comes first
                    _currentIsDelta = false;
                    _valid = true;
                    return;
                }
                _equalKeys = cmp == 0;
            }
            // delta is current (possibly shadowing the base entry at the same key)
            _value.Clear();
            bool exists = _batch.Classify(idx) switch
            {
                BatchLookupResult.Deleted => false,
                BatchLookupResult.MergeInProgress => _batch.ComputeValue(idx, _cfd.MergeOperator, _equalKeys, _equalKeys ? _base.Value : default, _value),
                _ => _batch.ComputeValue(idx, _cfd.MergeOperator, false, default, _value),
            };
            if (exists)
            {
                _currentIsDelta = true;
                _valid = true;
                return;
            }
            // deleted in the transaction: skip the key on both sides
            AdvanceDelta();
            if (_equalKeys) AdvanceBase();
        }
    }

    public void Dispose()
    {
        _base.Dispose();
        _value.Dispose();
    }
}
