using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// Merges several sorted internal iterators into one sorted stream. Port of MergingIterator
/// (table/merging_iterator.cc): a binary min-heap of children for forward iteration and a max-heap for reverse,
/// with upstream's direction-switch protocol (SwitchToForward / SwitchToBackward: re-seek every non-current child to
/// the current key and step past an equal key).
/// </summary>
/// <remarks>
/// <para><b>Range tombstones.</b> Upstream (since 7.x) feeds range tombstones into the heap so covered point keys
/// are skipped inside the merging iterator and whole covered ranges can be skipped with a reseek. This port keeps
/// the older design: the merging iterator only merges point keys and <see cref="DbIter"/> asks a
/// <see cref="RangeDelAggregator"/> whether each visible entry is covered (ShouldDelete). This is simpler and correct,
/// but a long range of deleted keys is still scanned key by key.</para>
/// <para><b>Allocation.</b> The heap is an <c>int[]</c> of child indices allocated once; positioning and stepping do not
/// allocate. Keys/values are the current child's spans. Async positioning (seeks, <see cref="NextAsync"/>,
/// <see cref="PrevAsync"/>) awaits children one at a time and completes synchronously when every child does; an async
/// direction switch re-positions the other children with async I/O (an async state machine, pooled).</para>
/// <para>Owns its children: disposing the merging iterator disposes them.</para>
/// </remarks>
public sealed class MergingIterator : InternalIterator
{
    private readonly InternalKeyComparator _icmp;
    private InternalIterator[] _children;
    private int[] _heap;
    private int _heapSize;
    private int _current = -1;
    private bool _forward = true;
    private bool _disposed;
    private Util.PooledBufferWriter? _switchKey; // the current key during an async direction switch

    /// <param name="comparator">Internal key comparator.</param>
    /// <param name="children">Child iterators (ownership is transferred).</param>
    public MergingIterator(InternalKeyComparator comparator, ReadOnlySpan<InternalIterator> children)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        _icmp = comparator;
        _children = children.ToArray();
        _heap = new int[Math.Max(1, _children.Length)];
    }

    /// <summary>
    /// Replaces the children (unpositioned afterwards) without disposing the previous ones: the owner of a pooled
    /// iterator graph (<see cref="IteratorGraph"/>) recycles them. Reuses the arrays when the child count is unchanged.
    /// </summary>
    internal void Reset(ReadOnlySpan<InternalIterator> children)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (children.Length == _children.Length)
        {
            children.CopyTo(_children);
        }
        else
        {
            _children = children.ToArray();
            _heap = new int[Math.Max(1, _children.Length)];
        }
        _heapSize = 0;
        _current = -1;
        _forward = true;
    }

    public int ChildCount => _children.Length;

    public override bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current >= 0;
    }

    public override ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ChildKey(_current);
    }

    public override ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ChildValue(_current);
    }

    /// <summary>Port of MergingIterator::SetPinnedItersMgr: passed to every child.</summary>
    public override void SetPinnedItersMgr(PinnedIteratorsManager? manager)
    {
        foreach (InternalIterator c in _children) c.SetPinnedItersMgr(manager);
    }

    public override bool TryGetPinnedValue(out PinnedSlice value) => _children[_current].TryGetPinnedValue(out value);

    /// <summary>Port of MergingIterator::UpperBoundCheckResult: the current child's.</summary>
    public override IterBoundCheck UpperBoundCheckResult => _current >= 0 ? _children[_current].UpperBoundCheckResult : IterBoundCheck.Unknown;

    internal override void SetIterateUpperBound(ReadOnlyMemory<byte>? upperBound)
    {
        foreach (InternalIterator c in _children) c.SetIterateUpperBound(upperBound);
    }

    /// <summary>Range-deletion skip: only the child providing the current entry moves (see the base method).</summary>
    public override bool SkipCoveredForward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (!_forward || !_children[_current].SkipCoveredForward(target, tombstoneSeq)) return false;
        AfterCurrentMoved();
        return true;
    }

    public override bool SkipCoveredBackward(ReadOnlySpan<byte> target, ulong tombstoneSeq)
    {
        if (_forward || !_children[_current].SkipCoveredBackward(target, tombstoneSeq)) return false;
        AfterCurrentMoved();
        return true;
    }

    public override ValueTask<bool> SkipCoveredForwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        _forward ? AfterSkip(_children[_current].SkipCoveredForwardAsync(target, tombstoneSeq, cancellationToken)) : new(false);

    public override ValueTask<bool> SkipCoveredBackwardAsync(ReadOnlyMemory<byte> target, ulong tombstoneSeq, CancellationToken cancellationToken = default) =>
        !_forward ? AfterSkip(_children[_current].SkipCoveredBackwardAsync(target, tombstoneSeq, cancellationToken)) : new(false);

    private ValueTask<bool> AfterSkip(ValueTask<bool> pending)
    {
        if (!pending.IsCompletedSuccessfully) return AfterSkipAsync(pending);
        if (!pending.Result) return new(false);
        AfterCurrentMoved();
        return new(true);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> AfterSkipAsync(ValueTask<bool> pending)
    {
        if (!await pending.ConfigureAwait(false)) return false;
        AfterCurrentMoved();
        return true;
    }

    // ------------------------------------------------------------------ positioning

    public override void SeekToFirst()
    {
        ClearHeap(forward: true);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            c.SeekToFirst();
            if (c.Valid) Push(i);
        }
        _current = Top();
    }

    public override void SeekToLast()
    {
        ClearHeap(forward: false);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            c.SeekToLast();
            if (c.Valid) Push(i);
        }
        _current = Top();
    }

    public override void Seek(ReadOnlySpan<byte> target)
    {
        ClearHeap(forward: true);
        PerfContext.Add(PerfMetric.SeekChildSeekCount, (ulong)_children.Length); // MergingIterator::SeekImpl, per child
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            c.Seek(target);
            if (c.Valid) Push(i);
        }
        _current = Top();
    }

    public override void SeekForPrev(ReadOnlySpan<byte> target)
    {
        ClearHeap(forward: false);
        PerfContext.Add(PerfMetric.SeekChildSeekCount, (ulong)_children.Length);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            c.SeekForPrev(target);
            if (c.Valid) Push(i);
        }
        _current = Top();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public override void Next()
    {
        Debug.Assert(Valid);
        // Ensure that all children are positioned after key(). If we are moving in the forward direction, it is
        // already true for all of the non-current children since current_ is the smallest child and key() ==
        // current_->key(). Otherwise, we explicitly position the non-current children.
        if (!_forward) SwitchToForward();
        ChildNext(_current);
        AfterCurrentMoved();
    }

    public override void Prev()
    {
        Debug.Assert(Valid);
        if (_forward) SwitchToBackward();
        _children[_current].Prev();
        AfterCurrentMoved();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
    private void AfterCurrentMoved()
    {
        Debug.Assert(_heapSize > 0 && _heap[0] == _current);
        if (ChildValid(_current))
        {
            // current is still valid after the move: fix the heap position.
            if (_heapSize > 1) SiftDown(0);
        }
        else
        {
            // current stopped being valid, remove it from the heap.
            PopTop();
        }
        _current = Top();
    }

    /// <summary>Port of MergingIterator::SwitchToForward.</summary>
    private void SwitchToForward()
    {
        ReadOnlySpan<byte> target = Key;
        int cur = _current;
        ClearHeap(forward: true);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            if (i != cur)
            {
                c.Seek(target);
                // child.key() is ensured to be > target after this step.
                if (c.Valid && _icmp.Compare(target, c.Key) == 0) c.Next();
            }
            if (c.Valid) Push(i);
        }
        _current = Top();
        Debug.Assert(_current == cur);
    }

    /// <summary>Port of MergingIterator::SwitchToBackward.</summary>
    private void SwitchToBackward()
    {
        ReadOnlySpan<byte> target = Key;
        int cur = _current;
        ClearHeap(forward: false);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            if (i != cur)
            {
                c.SeekForPrev(target);
                // child.key() is ensured to be < target after this step.
                if (c.Valid && _icmp.Compare(target, c.Key) == 0) c.Prev();
            }
            if (c.Valid) Push(i);
        }
        _current = Top();
        Debug.Assert(_current == cur);
    }

    // ------------------------------------------------------------------ async

    private enum PosOp : byte { First, Seek, Last, SeekForPrev }

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: true);
        return PositionChildrenAsync(0, default, PosOp.First, cancellationToken);
    }

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: true);
        return PositionChildrenAsync(0, target, PosOp.Seek, cancellationToken);
    }

    public override ValueTask SeekToLastAsync(CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: false);
        return PositionChildrenAsync(0, default, PosOp.Last, cancellationToken);
    }

    public override ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: false);
        return PositionChildrenAsync(0, target, PosOp.SeekForPrev, cancellationToken);
    }

    private static ValueTask PositionChild(InternalIterator c, ReadOnlyMemory<byte> target, PosOp op, CancellationToken ct) => op switch
    {
        PosOp.First => c.SeekToFirstAsync(ct),
        PosOp.Seek => c.SeekAsync(target, ct),
        PosOp.Last => c.SeekToLastAsync(ct),
        _ => c.SeekForPrevAsync(target, ct),
    };

    private ValueTask PositionChildrenAsync(int start, ReadOnlyMemory<byte> target, PosOp op, CancellationToken ct)
    {
        for (int i = start; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            ValueTask t = PositionChild(c, target, op, ct);
            if (!t.IsCompletedSuccessfully) return PositionChildrenSlowAsync(t, i, target, op, ct);
            t.GetAwaiter().GetResult();
            if (c.Valid) Push(i);
        }
        _current = Top();
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionChildrenSlowAsync(ValueTask pending, int index, ReadOnlyMemory<byte> target, PosOp op, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (_children[index].Valid) Push(index);
        await PositionChildrenAsync(index + 1, target, op, ct).ConfigureAwait(false);
    }

    public override ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        Debug.Assert(Valid);
        if (!_forward) return SwitchThenMoveAsync(forward: true, cancellationToken);
        ValueTask t = _children[_current].NextAsync(cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            t.GetAwaiter().GetResult();
            AfterCurrentMoved();
            return ValueTask.CompletedTask;
        }
        return AwaitNextAsync(t);
    }

    public override ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        Debug.Assert(Valid);
        if (_forward) return SwitchThenMoveAsync(forward: false, cancellationToken);
        ValueTask t = _children[_current].PrevAsync(cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            t.GetAwaiter().GetResult();
            AfterCurrentMoved();
            return ValueTask.CompletedTask;
        }
        return AwaitNextAsync(t);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitNextAsync(ValueTask pending)
    {
        await pending.ConfigureAwait(false);
        AfterCurrentMoved();
    }

    /// <summary>
    /// Async SwitchToForward / SwitchToBackward followed by the Next / Prev of the current child: every non-current child
    /// is re-positioned with async I/O. The current key is copied first (it must outlive the awaits).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SwitchThenMoveAsync(bool forward, CancellationToken ct)
    {
        Util.PooledBufferWriter key = _switchKey ??= new Util.PooledBufferWriter(64);
        key.Clear();
        key.Write(Key);
        ReadOnlyMemory<byte> target = key.WrittenMemory;
        int cur = _current;
        ClearHeap(forward);
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            if (i != cur)
            {
                if (forward)
                {
                    await c.SeekAsync(target, ct).ConfigureAwait(false);
                    // child.key() is ensured to be > target after this step.
                    if (c.Valid && _icmp.Compare(target.Span, c.Key) == 0) await c.NextAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    await c.SeekForPrevAsync(target, ct).ConfigureAwait(false);
                    // child.key() is ensured to be < target after this step.
                    if (c.Valid && _icmp.Compare(target.Span, c.Key) == 0) await c.PrevAsync(ct).ConfigureAwait(false);
                }
            }
            if (c.Valid) Push(i);
        }
        _current = Top();
        Debug.Assert(_current == cur);
        InternalIterator current = _children[cur];
        if (forward) await current.NextAsync(ct).ConfigureAwait(false);
        else await current.PrevAsync(ct).ConfigureAwait(false);
        AfterCurrentMoved();
    }

    // ------------------------------------------------------------------ children
    // The children are almost always table iterators (L0 files) and level iterators: test for those sealed types so
    // the per-key calls are direct (and inlined) instead of virtual, independent of the JIT's profile-guided
    // devirtualization (which the AggressiveOptimization methods here do not get). Upstream caches the child key in
    // IteratorWrapper for the same reason.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> ChildKey(int i)
    {
        InternalIterator c = _children[i];
        if (c is TableInternalIterator t) return t.Key;
        if (c is LevelIterator l) return l.Key;
        return c.Key;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ReadOnlySpan<byte> ChildValue(int i)
    {
        InternalIterator c = _children[i];
        if (c is TableInternalIterator t) return t.Value;
        if (c is LevelIterator l) return l.Value;
        return c.Value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ChildValid(int i)
    {
        InternalIterator c = _children[i];
        if (c is TableInternalIterator t) return t.Valid;
        if (c is LevelIterator l) return l.Valid;
        return c.Valid;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ChildNext(int i)
    {
        InternalIterator c = _children[i];
        if (c is TableInternalIterator t) t.Next();
        else if (c is LevelIterator l) l.Next();
        else c.Next();
    }

    // ------------------------------------------------------------------ heap (port of util/heap.h BinaryHeap)

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClearHeap(bool forward)
    {
        _heapSize = 0;
        _current = -1;
        _forward = forward;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Top() => _heapSize > 0 ? _heap[0] : -1;

    /// <summary>True if child <paramref name="a"/> should be above child <paramref name="b"/> in the heap.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Before(int a, int b)
    {
        int c = _icmp.Compare(ChildKey(a), ChildKey(b));
        return _forward ? c < 0 : c > 0;
    }

    private void Push(int child)
    {
        int i = _heapSize++;
        _heap[i] = child;
        // sift up
        while (i > 0)
        {
            int parent = (i - 1) >> 1;
            if (!Before(_heap[i], _heap[parent])) break;
            (_heap[i], _heap[parent]) = (_heap[parent], _heap[i]);
            i = parent;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void PopTop()
    {
        _heapSize--;
        if (_heapSize > 0)
        {
            _heap[0] = _heap[_heapSize];
            SiftDown(0);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void SiftDown(int index)
    {
        int[] heap = _heap;
        int n = _heapSize;
        int v = heap[index];
        while (true)
        {
            int left = 2 * index + 1;
            if (left >= n) break;
            int right = left + 1;
            int best = right < n && Before(heap[right], heap[left]) ? right : left;
            if (!Before(heap[best], v)) break;
            heap[index] = heap[best];
            index = best;
        }
        heap[index] = v;
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed) return;
        _disposed = true;
        foreach (InternalIterator c in _children) c.Dispose();
        _switchKey?.Dispose();
    }
}
