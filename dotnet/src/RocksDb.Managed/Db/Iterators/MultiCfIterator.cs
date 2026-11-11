using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>One column family's columns at the current key of an <see cref="AttributeGroupIterator"/> (rocksdb::IteratorAttributeGroup).</summary>
public readonly struct IteratorAttributeGroup
{
    internal IteratorAttributeGroup(ColumnFamily columnFamily, WideColumns columns)
    {
        ColumnFamily = columnFamily;
        Columns = columns;
    }

    public ColumnFamily ColumnFamily { get; }

    /// <summary>The key's columns in <see cref="ColumnFamily"/> (a plain value is a single default column). Valid until the iterator moves.</summary>
    public WideColumns Columns { get; }
}

/// <summary>
/// Shared machinery of the cross-column-family iterators. Port of MultiCfIteratorImpl (db/multi_cf_iterator_impl.h):
/// one child <see cref="Iterator"/> per column family, all reading one consistent view; the iterator is positioned at
/// the smallest (forward) or largest (reverse) child key, and the children positioned at that key (in column family
/// order) are the ones whose values are combined. Upstream keeps the children in a binary heap; with the handful of
/// column families these iterators are used with, the children are scanned linearly instead (same order and
/// semantics). Switching direction re-seeks every child to the current key like upstream (Next after Prev = Seek(key)
/// then advance). Not thread-safe; dispose to release the children (and the implicit snapshot).
/// </summary>
public abstract class MultiCfIteratorBase : IDisposable
{
    private protected readonly ColumnFamily[] _cfs;
    private protected readonly Iterator[] _children;
    // Children positioned at the current key, in column family order (to_populate).
    private protected readonly int[] _current;
    private protected int _currentCount;
    private readonly Comparator _cmp;
    private readonly Snapshot? _ownedSnapshot;
    private readonly PooledBufferWriter _key = new(64);
    // The current key copied for the re-seek of a direction switch (Seek rewrites _key while the children move).
    private readonly PooledBufferWriter _switchKey = new(64);
    private bool _forward = true;
    private bool _disposed;

    private protected MultiCfIteratorBase(RocksDatabase db, ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        ArgumentNullException.ThrowIfNull(db);
        // DBImpl::NewMultiCfIterator argument checks.
        if (columnFamilies.IsEmpty) ThrowHelper.InvalidArgument("No Column Family was provided");
        Comparator first = columnFamilies[0].Data.UserComparator;
        for (int i = 1; i < columnFamilies.Length; i++)
        {
            Comparator c = columnFamilies[i].Data.UserComparator;
            if (!ReferenceEquals(first, c) && first.Name != c.Name) ThrowHelper.InvalidArgument("Different comparators are being used across CFs");
        }
        _cmp = first.TimestampSize == 0 ? first : first.RootComparator;
        _cfs = columnFamilies.ToArray();
        _children = new Iterator[_cfs.Length];
        _current = new int[_cfs.Length];
        // DBImpl::NewIterators: one consistent view across the column families. With an explicit snapshot that is the
        // snapshot; otherwise an implicit snapshot owned by this iterator.
        ReadOptions? ro = options;
        if (_cfs.Length > 1 && options?.Snapshot is null)
        {
            _ownedSnapshot = db.GetSnapshot();
            ro = options?.Clone() ?? new ReadOptions();
            ro.Snapshot = _ownedSnapshot;
        }
        try
        {
            for (int i = 0; i < _cfs.Length; i++)
            {
                if (_cfs[i].Db != db.Impl) ThrowHelper.InvalidArgument("column family does not belong to this DB");
                _children[i] = _cfs[i].NewIterator(ro);
            }
        }
        catch
        {
            DisposeChildren();
            throw;
        }
    }

    /// <summary>True if positioned at a key.</summary>
    public bool Valid => _currentCount > 0;

    /// <summary>The current key. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Key => _key.WrittenSpan;

    public void SeekToFirst() => SeekCommon(forward: true, static (it, _) => it.SeekToFirst(), default);
    public void SeekToLast() => SeekCommon(forward: false, static (it, _) => it.SeekToLast(), default);
    /// <summary>Positions at the first key &gt;= <paramref name="target"/> in any of the column families.</summary>
    public void Seek(ReadOnlySpan<byte> target) => SeekCommon(forward: true, static (it, t) => it.Seek(t), target);
    /// <summary>Positions at the last key &lt;= <paramref name="target"/> in any of the column families.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target) => SeekCommon(forward: false, static (it, t) => it.SeekForPrev(t), target);

    public void Next()
    {
        ThrowIfInvalid();
        if (!_forward)
        {
            // GetHeap<MultiCfMinHeap>: re-seek every child to the current key in the forward direction.
            Seek(CopySwitchKey());
            if (!Valid) return;
        }
        Advance(static it => it.Next());
    }

    public void Prev()
    {
        ThrowIfInvalid();
        if (_forward)
        {
            SeekForPrev(CopySwitchKey());
            if (!Valid) return;
        }
        Advance(static it => it.Prev());
    }

    private ReadOnlySpan<byte> CopySwitchKey()
    {
        _switchKey.Clear();
        _switchKey.Write(_key.WrittenSpan);
        return _switchKey.WrittenSpan;
    }

    private delegate void SeekAction(Iterator it, ReadOnlySpan<byte> target);

    private void SeekCommon(bool forward, SeekAction seek, ReadOnlySpan<byte> target)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _forward = forward;
        // A child that fails (throws) leaves the iterator invalid (considerStatus + heap.clear() upstream).
        _currentCount = 0;
        foreach (Iterator it in _children) seek(it, target);
        Position();
    }

    /// <summary>AdvanceIterator: moves every child at the current key (the top and its equals) one step.</summary>
    private void Advance(Action<Iterator> step)
    {
        // A child that fails (throws) leaves the iterator invalid (considerStatus + heap.clear() upstream).
        int n = _currentCount;
        _currentCount = 0;
        for (int i = 0; i < n; i++) step(_children[_current[i]]);
        Position();
    }

    /// <summary>Finds the top key among the valid children and the children at it (PopulateIterator's grouping).</summary>
    private void Position()
    {
        OnReset();
        _currentCount = 0;
        int top = -1;
        for (int i = 0; i < _children.Length; i++)
        {
            Iterator it = _children[i];
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

    /// <summary>ResetFunc: drops the values computed for the previous position.</summary>
    private protected abstract void OnReset();

    private void DisposeChildren()
    {
        foreach (Iterator? it in _children) it?.Dispose();
        _ownedSnapshot?.Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _currentCount = 0;
        DisposeChildren();
        _key.Dispose();
        _switchKey.Dispose();
        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// A cross-column-family iterator that coalesces the wide columns of each key across the column families. Port of
/// CoalescingIterator (db/coalescing_iterator.{h,cc}): every key present in any of the column families is visited
/// once, in comparator order; its <see cref="Columns"/> are the union of the key's columns in all column families,
/// sorted by name, where a column present in several column families takes the value from the one listed last
/// (later column families shadow earlier ones column by column). <see cref="Value"/> is the coalesced default column
/// (empty if none). Created by <see cref="RocksDatabase.NewCoalescingIterator(ReadOptions?, ReadOnlySpan{ColumnFamily})"/>.
/// </summary>
public sealed class CoalescingIterator : MultiCfIteratorBase
{
    private readonly WideColumns _columns = new();
    private readonly PooledBufferWriter _entity = new(256);
    private readonly List<(WideColumn Column, int Order)> _merge = new(16);
    private WideColumn[] _sorted = new WideColumn[16];
    private bool _populated;

    internal CoalescingIterator(RocksDatabase db, ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
        : base(db, options, columnFamilies)
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

    private protected override void OnReset()
    {
        _populated = false;
    }

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
        // Min-heap order of WideColumnWithOrderComparator: by name, then by column family order; the last entry of a
        // run of equal names (the latest column family) wins.
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
        // Own the coalesced bytes so the result does not depend on the children's buffers.
        _columns.SetSerialized(_entity.WrittenSpan);
        Array.Clear(_sorted, 0, n);
        _merge.Clear();
        _populated = true;
    }
}

/// <summary>
/// A cross-column-family iterator returning, for each key, its attribute groups: the key's columns in each column
/// family that has it. Port of AttributeGroupIteratorImpl (db/attribute_group_iterator_impl.{h,cc}). Created by
/// <see cref="RocksDatabase.NewAttributeGroupIterator(ReadOptions?, ReadOnlySpan{ColumnFamily})"/>.
/// </summary>
public sealed class AttributeGroupIterator : MultiCfIteratorBase
{
    private readonly List<IteratorAttributeGroup> _groups = new(4);
    private bool _populated;

    internal AttributeGroupIterator(RocksDatabase db, ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
        : base(db, options, columnFamilies)
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
                // AddToAttributeGroups.
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

/// <summary>Cross-column-family iterators (DB::NewCoalescingIterator / DB::NewAttributeGroupIterator).</summary>
public sealed partial class RocksDatabase
{
    /// <summary>
    /// A <see cref="CoalescingIterator"/> over <paramref name="columnFamilies"/> (DB::NewCoalescingIterator), reading
    /// one consistent view (the snapshot of <paramref name="options"/>, or an implicit one held by the iterator).
    /// InvalidArgument when no column family is given or their comparators differ.
    /// </summary>
    public CoalescingIterator NewCoalescingIterator(ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        _db.ThrowIfDisposed();
        return new CoalescingIterator(this, options, columnFamilies);
    }

    /// <summary>An <see cref="AttributeGroupIterator"/> over <paramref name="columnFamilies"/> (DB::NewAttributeGroupIterator); see <see cref="NewCoalescingIterator"/>.</summary>
    public AttributeGroupIterator NewAttributeGroupIterator(ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        _db.ThrowIfDisposed();
        return new AttributeGroupIterator(this, options, columnFamilies);
    }
}
