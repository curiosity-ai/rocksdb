using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Cross-column-family iterators over a transaction's view (TransactionBaseImpl::GetCoalescingIterator /
/// GetAttributeGroupIterator, utilities/transactions/transaction_base.cc): MultiCfIteratorImpl
/// (db/multi_cf_iterator_impl.h) over one <see cref="TransactionIterator"/> per column family (the DB iterator merged
/// with the transaction's writes). Same positioning as <see cref="MultiCfIteratorBase"/>: the iterator is at the smallest
/// (forward) or largest (reverse) child key, the children at that key (in column family order) are combined, and
/// switching direction re-seeks every child to the current key. Not thread-safe; dispose before the transaction.
/// </summary>
public abstract class TransactionMultiCfIterator : IDisposable
{
    private protected readonly ColumnFamily[] _cfs;
    private protected TransactionIterator[] _children = [];
    private protected readonly int[] _current;
    private protected int _currentCount;
    private readonly Comparator _cmp;
    private Snapshot? _ownedSnapshot;
    private readonly PooledBufferWriter _key = new(64);
    private bool _forward = true;
    private bool _disposed;

    private protected TransactionMultiCfIterator(ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        _cfs = columnFamilies.ToArray();
        _current = new int[_cfs.Length];
        Comparator first = _cfs[0].Data.UserComparator;
        _cmp = first.TimestampSize == 0 ? first : first.RootComparator;
    }

    internal ReadOnlySpan<ColumnFamily> ColumnFamilies => _cfs;

    internal void Init(TransactionIterator[] children, Snapshot? ownedSnapshot)
    {
        _children = children;
        _ownedSnapshot = ownedSnapshot;
    }

    /// <summary>True if positioned at a key.</summary>
    public bool Valid => _currentCount > 0;

    /// <summary>The current key. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Key => _key.WrittenSpan;

    public void SeekToFirst()
    {
        Begin(forward: true);
        foreach (TransactionIterator it in _children) it.SeekToFirst();
        Position();
    }

    public void SeekToLast()
    {
        Begin(forward: false);
        foreach (TransactionIterator it in _children) it.SeekToLast();
        Position();
    }

    /// <summary>Positions at the first key &gt;= <paramref name="target"/> in any of the column families.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        Begin(forward: true);
        foreach (TransactionIterator it in _children) it.Seek(target);
        Position();
    }

    /// <summary>Positions at the last key &lt;= <paramref name="target"/> in any of the column families.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        Begin(forward: false);
        foreach (TransactionIterator it in _children) it.SeekForPrev(target);
        Position();
    }

    public void Next()
    {
        ThrowIfInvalid();
        if (!_forward)
        {
            // Re-seek every child to the current key in the forward direction.
            Seek(_key.WrittenSpan.ToArray());
            if (!Valid) return;
        }
        for (int i = 0; i < _currentCount; i++) _children[_current[i]].Next();
        Position();
    }

    public void Prev()
    {
        ThrowIfInvalid();
        if (_forward)
        {
            SeekForPrev(_key.WrittenSpan.ToArray());
            if (!Valid) return;
        }
        for (int i = 0; i < _currentCount; i++) _children[_current[i]].Prev();
        Position();
    }

    private void Begin(bool forward)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _forward = forward;
    }

    /// <summary>Finds the top key among the valid children and the children at it.</summary>
    private void Position()
    {
        OnReset();
        _currentCount = 0;
        int top = -1;
        for (int i = 0; i < _children.Length; i++)
        {
            TransactionIterator it = _children[i];
            if (!it.Valid) continue;
            if (top < 0)
            {
                top = i;
                _current[_currentCount++] = i;
                continue;
            }
            int c = _cmp.Compare(it.Key, _children[top].Key);
            if (!_forward) c = -c;
            if (c < 0)
            {
                top = i;
                _currentCount = 0;
                _current[_currentCount++] = i;
            }
            else if (c == 0)
            {
                _current[_currentCount++] = i;
            }
        }
        _key.Clear();
        if (top >= 0) _key.Write(_children[top].Key);
    }

    private protected void ThrowIfInvalid()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_currentCount == 0) throw new InvalidOperationException("The iterator is not positioned at a key.");
    }

    /// <summary>Drops the values computed for the previous position.</summary>
    private protected abstract void OnReset();

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _currentCount = 0;
        foreach (TransactionIterator it in _children) it.Dispose();
        _ownedSnapshot?.Dispose();
        _key.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Coalescing iterator over a transaction's view (CoalescingIterator, db/coalescing_iterator.cc): each key once, its
/// columns the union across the column families, a column present in several taking the value of the one listed last.
/// Created by <see cref="Transaction.GetCoalescingIterator"/>.
/// </summary>
public sealed class TransactionCoalescingIterator : TransactionMultiCfIterator
{
    private readonly WideColumns _columns = new();
    private readonly PooledBufferWriter _entity = new(256);
    private readonly List<(WideColumn Column, int Order)> _merge = new(16);
    private WideColumn[] _sorted = new WideColumn[16];
    private bool _populated;

    internal TransactionCoalescingIterator(ReadOnlySpan<ColumnFamily> columnFamilies) : base(columnFamilies)
    {
    }

    /// <summary>The coalesced default column value. REQUIRES: Valid. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Value => Columns.DefaultColumnValue;

    /// <summary>The coalesced columns of the current key. REQUIRES: Valid. The object is reused; valid until the iterator moves.</summary>
    public WideColumns Columns
    {
        get
        {
            ThrowIfInvalid();
            if (!_populated) Coalesce();
            return _columns;
        }
    }

    private protected override void OnReset() => _populated = false;

    /// <summary>CoalescingIterator::Coalesce.</summary>
    private void Coalesce()
    {
        _merge.Clear();
        for (int k = 0; k < _currentCount; k++)
        {
            int order = _current[k];
            WideColumns c = _children[order].Columns;
            for (int i = 0; i < c.Count; i++) _merge.Add((c[i], order));
        }
        // By name, then by column family order; the last entry of a run of equal names (the latest column family) wins.
        _merge.Sort(static (a, b) =>
        {
            int c = a.Column.Name.Span.SequenceCompareTo(b.Column.Name.Span);
            return c != 0 ? c : a.Order.CompareTo(b.Order);
        });
        int n = 0;
        if (_sorted.Length < _merge.Count) _sorted = new WideColumn[Math.Max(_merge.Count, _sorted.Length * 2)];
        for (int i = 0; i < _merge.Count; i++)
        {
            if (i + 1 < _merge.Count && _merge[i].Column.Name.Span.SequenceEqual(_merge[i + 1].Column.Name.Span)) continue;
            _sorted[n++] = _merge[i].Column;
        }
        _entity.Clear();
        Db.Wide.WideColumnSerialization.Serialize(_sorted.AsSpan(0, n), _entity);
        _columns.SetSerialized(_entity.WrittenSpan);
        Array.Clear(_sorted, 0, n);
        _merge.Clear();
        _populated = true;
    }
}

/// <summary>
/// Attribute group iterator over a transaction's view (AttributeGroupIteratorImpl): for each key, its columns in each
/// column family that has it. Created by <see cref="Transaction.GetAttributeGroupIterator"/>.
/// </summary>
public sealed class TransactionAttributeGroupIterator : TransactionMultiCfIterator
{
    private readonly List<IteratorAttributeGroup> _groups = new(4);
    private bool _populated;

    internal TransactionAttributeGroupIterator(ReadOnlySpan<ColumnFamily> columnFamilies) : base(columnFamilies)
    {
    }

    /// <summary>
    /// The attribute groups of the current key, in the order the column families were given (only those having the
    /// key). REQUIRES: Valid. The list and the column objects are reused; valid until the iterator moves.
    /// </summary>
    public IReadOnlyList<IteratorAttributeGroup> AttributeGroups
    {
        get
        {
            ThrowIfInvalid();
            if (!_populated)
            {
                for (int k = 0; k < _currentCount; k++)
                {
                    int i = _current[k];
                    _groups.Add(new IteratorAttributeGroup(_cfs[i], _children[i].Columns));
                }
                _populated = true;
            }
            return _groups;
        }
    }

    private protected override void OnReset()
    {
        _groups.Clear();
        _populated = false;
    }
}
