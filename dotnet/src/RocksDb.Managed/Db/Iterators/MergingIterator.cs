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
/// allocate. Keys/values are the current child's spans. Async positioning (<see cref="SeekAsync"/>,
/// <see cref="SeekToFirstAsync"/>, <see cref="NextAsync"/>) awaits children one at a time and completes
/// synchronously when every child does; direction switches and reverse movement are synchronous.</para>
/// <para>Owns its children: disposing the merging iterator disposes them.</para>
/// </remarks>
public sealed class MergingIterator : InternalIterator
{
    private readonly InternalKeyComparator _icmp;
    private readonly InternalIterator[] _children;
    private readonly int[] _heap;
    private int _heapSize;
    private int _current = -1;
    private bool _forward = true;
    private bool _disposed;

    /// <param name="comparator">Internal key comparator.</param>
    /// <param name="children">Child iterators (ownership is transferred).</param>
    public MergingIterator(InternalKeyComparator comparator, ReadOnlySpan<InternalIterator> children)
    {
        ArgumentNullException.ThrowIfNull(comparator);
        _icmp = comparator;
        _children = children.ToArray();
        _heap = new int[Math.Max(1, _children.Length)];
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
        get => _children[_current].Key;
    }

    public override ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _children[_current].Value;
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
        for (int i = 0; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            c.SeekForPrev(target);
            if (c.Valid) Push(i);
        }
        _current = Top();
    }

    public override void Next()
    {
        Debug.Assert(Valid);
        // Ensure that all children are positioned after key(). If we are moving in the forward direction, it is
        // already true for all of the non-current children since current_ is the smallest child and key() ==
        // current_->key(). Otherwise, we explicitly position the non-current children.
        if (!_forward) SwitchToForward();
        _children[_current].Next();
        AfterCurrentMoved();
    }

    public override void Prev()
    {
        Debug.Assert(Valid);
        if (_forward) SwitchToBackward();
        _children[_current].Prev();
        AfterCurrentMoved();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AfterCurrentMoved()
    {
        Debug.Assert(_heapSize > 0 && _heap[0] == _current);
        if (_children[_current].Valid)
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

    public override ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: true);
        return PositionChildrenAsync(0, default, seekToFirst: true, cancellationToken);
    }

    public override ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        ClearHeap(forward: true);
        return PositionChildrenAsync(0, target, seekToFirst: false, cancellationToken);
    }

    private ValueTask PositionChildrenAsync(int start, ReadOnlyMemory<byte> target, bool seekToFirst, CancellationToken ct)
    {
        for (int i = start; i < _children.Length; i++)
        {
            InternalIterator c = _children[i];
            ValueTask t = seekToFirst ? c.SeekToFirstAsync(ct) : c.SeekAsync(target, ct);
            if (!t.IsCompletedSuccessfully) return PositionChildrenSlowAsync(t, i, target, seekToFirst, ct);
            t.GetAwaiter().GetResult();
            if (c.Valid) Push(i);
        }
        _current = Top();
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionChildrenSlowAsync(ValueTask pending, int index, ReadOnlyMemory<byte> target, bool seekToFirst, CancellationToken ct)
    {
        await pending.ConfigureAwait(false);
        if (_children[index].Valid) Push(index);
        await PositionChildrenAsync(index + 1, target, seekToFirst, ct).ConfigureAwait(false);
    }

    public override ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        Debug.Assert(Valid);
        if (!_forward) SwitchToForward();
        ValueTask t = _children[_current].NextAsync(cancellationToken);
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
        int c = _icmp.Compare(_children[a].Key, _children[b].Key);
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

    private void PopTop()
    {
        _heapSize--;
        if (_heapSize > 0)
        {
            _heap[0] = _heap[_heapSize];
            SiftDown(0);
        }
    }

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
    }
}
