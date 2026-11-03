using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table;

/// <summary>
/// Iterator over the table's index yielding data block handles; transparently handles a two-level (partitioned)
/// index. Port of the index-iteration parts of BinarySearchIndexReader / PartitionedIndexIterator
/// (table/block_based/partitioned_index_iterator.cc). The synchronous moves load index partitions synchronously; the
/// async moves (<see cref="SeekAsync"/>, <see cref="NextAsync"/>, ...) read a partition that is not cached with async
/// I/O (completing synchronously on a cache hit or a pinned partition).
/// </summary>
internal sealed class IndexCursor
{
    [ThreadStatic] private static IndexCursor? t_cached;

    private BlockBasedTableReader _reader = null!;
    private readonly BlockIter _top = new();
    private readonly BlockIter _part = new();
    private BlockRef _topRef;
    private BlockRef _partRef;
    private bool _partitioned;
    private TableReadFlags _flags;

    public static IndexCursor Rent(BlockBasedTableReader reader, TableReadFlags flags)
    {
        IndexCursor c = t_cached ?? new IndexCursor();
        t_cached = null;
        c.Init(reader, flags);
        return c;
    }

    public void Return()
    {
        Reset();
        t_cached = this;
    }

    public void Init(BlockBasedTableReader reader, TableReadFlags flags)
    {
        UnloadPartition();
        ReleaseTop();
        _reader = reader;
        _flags = flags;
        _partitioned = reader.IsPartitionedIndex;
        _topRef = reader.GetIndexBlockRef(flags);
        reader.InitIndexIter(_top, _topRef.Block!, isPartition: false);
    }

    public void Reset()
    {
        UnloadPartition();
        ReleaseTop();
        _reader = null!;
    }

    private void ReleaseTop()
    {
        _top.Clear();
        if (!_topRef.IsEmpty) _topRef.Release();
    }

    /// <summary>Releases the partition block and returns key buffers to the pool.</summary>
    public void Dispose()
    {
        Reset();
        _top.Dispose();
        _part.Dispose();
    }

    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _partitioned ? !_partRef.IsEmpty && _part.Valid : _top.Valid;
    }

    public BlockHandle Handle => _partitioned ? _part.BlockHandle : _top.BlockHandle;
    public ReadOnlySpan<byte> Key { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _partitioned ? _part.Key : _top.Key; }

    public void Seek(ReadOnlySpan<byte> target)
    {
        _top.Seek(target);
        if (!_partitioned) return;
        if (!_top.Valid) { UnloadPartition(); return; }
        LoadPartition();
        _part.Seek(target);
        SkipEmptyForward();
    }

    /// <summary>
    /// Index seek of a point lookup: with a kHashSearch prefix index (prefix seek, BlockBasedTable::Get without
    /// total_order_seek) the index iterator is positioned through the prefix's candidate blocks and left invalid when
    /// the prefix is absent; otherwise a regular <see cref="Seek"/>.
    /// </summary>
    public void SeekForGet(ReadOnlySpan<byte> target)
    {
        if (!_partitioned && _reader.TryGetPrefixBlocks(target, out ReadOnlySpan<uint> blocks))
        {
            _top.PrefixSeek(target, blocks);
            return;
        }
        Seek(target);
    }

    public void SeekToFirst()
    {
        _top.SeekToFirst();
        if (!_partitioned) return;
        if (!_top.Valid) { UnloadPartition(); return; }
        LoadPartition();
        _part.SeekToFirst();
        SkipEmptyForward();
    }

    public void SeekToLast()
    {
        _top.SeekToLast();
        if (!_partitioned) return;
        if (!_top.Valid) { UnloadPartition(); return; }
        LoadPartition();
        _part.SeekToLast();
        SkipEmptyBackward();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Next()
    {
        if (!_partitioned) { _top.Next(); return; }
        _part.Next();
        SkipEmptyForward();
    }

    public void Prev()
    {
        if (!_partitioned) { _top.Prev(); return; }
        _part.Prev();
        SkipEmptyBackward();
    }

    private void SkipEmptyForward()
    {
        while (!_part.Valid)
        {
            _top.Next();
            if (!_top.Valid) { UnloadPartition(); return; }
            LoadPartition();
            _part.SeekToFirst();
        }
    }

    private void SkipEmptyBackward()
    {
        while (!_part.Valid)
        {
            _top.Prev();
            if (!_top.Valid) { UnloadPartition(); return; }
            LoadPartition();
            _part.SeekToLast();
        }
    }

    private void LoadPartition()
    {
        UnloadPartition();
        _partRef = _reader.GetBlockRef(_top.BlockHandle, BlockKind.Index, _flags);
        _reader.InitIndexIter(_part, _partRef.Block!, isPartition: true);
    }

    // ---- async moves: partitions read with async I/O ----

    /// <summary>How to position inside a newly loaded partition.</summary>
    private enum PartOp : byte { Seek, First, Last }

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        _top.Seek(target.Span);
        return AfterTopMoveAsync(PartOp.Seek, target, ct);
    }

    /// <summary>Async <see cref="SeekForGet"/> (the hash index prefix seek is in memory).</summary>
    public ValueTask SeekForGetAsync(ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        if (!_partitioned && _reader.TryGetPrefixBlocks(target.Span, out ReadOnlySpan<uint> blocks))
        {
            _top.PrefixSeek(target.Span, blocks);
            return ValueTask.CompletedTask;
        }
        return SeekAsync(target, ct);
    }

    public ValueTask SeekToFirstAsync(CancellationToken ct)
    {
        _top.SeekToFirst();
        return AfterTopMoveAsync(PartOp.First, default, ct);
    }

    public ValueTask SeekToLastAsync(CancellationToken ct)
    {
        _top.SeekToLast();
        return AfterTopMoveAsync(PartOp.Last, default, ct);
    }

    public ValueTask NextAsync(CancellationToken ct)
    {
        if (!_partitioned)
        {
            _top.Next();
            return ValueTask.CompletedTask;
        }
        _part.Next();
        return SkipEmptyAsync(forward: true, ct);
    }

    public ValueTask PrevAsync(CancellationToken ct)
    {
        if (!_partitioned)
        {
            _top.Prev();
            return ValueTask.CompletedTask;
        }
        _part.Prev();
        return SkipEmptyAsync(forward: false, ct);
    }

    /// <summary>After a move of the top-level index: load the partition it points to and position in it.</summary>
    private ValueTask AfterTopMoveAsync(PartOp op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        if (!_partitioned) return ValueTask.CompletedTask;
        if (!_top.Valid)
        {
            UnloadPartition();
            return ValueTask.CompletedTask;
        }
        UnloadPartition();
        ValueTask<BlockRef> pending = _reader.GetBlockRefAsync(_top.BlockHandle, BlockKind.Index, _flags, ct);
        if (!pending.IsCompletedSuccessfully) return LoadPartitionSlowAsync(pending, op, target, ct);
        PositionInPartition(pending.Result, op, target);
        return SkipEmptyAsync(op != PartOp.Last, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask LoadPartitionSlowAsync(ValueTask<BlockRef> pending, PartOp op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        PositionInPartition(await pending.ConfigureAwait(false), op, target);
        await SkipEmptyAsync(op != PartOp.Last, ct).ConfigureAwait(false);
    }

    private void PositionInPartition(BlockRef partition, PartOp op, ReadOnlyMemory<byte> target)
    {
        _partRef = partition;
        _reader.InitIndexIter(_part, _partRef.Block!, isPartition: true);
        switch (op)
        {
            case PartOp.Seek: _part.Seek(target.Span); break;
            case PartOp.First: _part.SeekToFirst(); break;
            default: _part.SeekToLast(); break;
        }
    }

    /// <summary>Async <see cref="SkipEmptyForward"/> / <see cref="SkipEmptyBackward"/>.</summary>
    private ValueTask SkipEmptyAsync(bool forward, CancellationToken ct)
    {
        if (_part.Valid) return ValueTask.CompletedTask;
        if (forward) _top.Next();
        else _top.Prev();
        return AfterTopMoveAsync(forward ? PartOp.First : PartOp.Last, default, ct);
    }

    private void UnloadPartition()
    {
        _part.Clear();
        if (!_partRef.IsEmpty) _partRef.Release();
    }
}

/// <summary>
/// Two-level iterator over a block-based table: index → data blocks. Port of BlockBasedTableIterator
/// (table/block_based/block_based_table_iterator.cc) without readahead or bounds checks. With prefix options
/// (<see cref="TableIteratorPrefixOptions.CheckFilter"/>), Seek and SeekForPrev first consult the prefix filter
/// (CheckPrefixMayMatch) and leave the iterator invalid without reading any data block when it excludes the prefix.
/// <para>
/// Keys/values are spans into pinned block buffers (or the iterator's key buffer) valid until the next move.
/// The async variants (forward and reverse) only await when a data block or an index partition has to be read from the
/// file (cache hits complete synchronously). Not thread-safe; dispose to release the current block.
/// </para>
/// </summary>
public sealed class BlockBasedTableIterator : IDisposable
{
    private BlockBasedTableReader _reader;
    private readonly IndexCursor _index = new();
    private readonly BlockIter _data = new();
    private readonly TableReadFlags _flags;
    private TableIteratorPrefixOptions _prefix;
    private BlockRef _block;
    private BlockHandle _blockHandle;
    private Db.Iterators.PinnedIteratorsManager? _pinMgr;
    private Env.AsyncPrefetchBuffer? _prefetch;

    internal BlockBasedTableIterator(BlockBasedTableReader reader, TableReadFlags flags, in TableIteratorPrefixOptions prefixOptions)
    {
        _reader = reader;
        _flags = flags;
        _prefix = prefixOptions;
        _index.Init(reader, flags);
    }

    /// <summary>
    /// Port of CheckPrefixMayMatch: false when the prefix filter rules out <paramref name="target"/>'s prefix (the
    /// caller then invalidates the iterator).
    /// </summary>
    private bool CheckPrefixMayMatch(ReadOnlySpan<byte> target, bool backward)
    {
        if (_prefix.NeedUpperBoundCheck && backward)
        {
            // Upper bound check isn't sufficient for backward direction to guarantee the same result as total order,
            // so disable prefix check.
            return true;
        }
        if (_prefix.CheckFilter && !_reader.PrefixRangeMayMatch(target, _prefix, _flags))
        {
            _reader.RecordSeekFiltered();
            return false;
        }
        return true;
    }

    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => !_block.IsEmpty && _data.Valid;
    }

    /// <summary>Current internal key.</summary>
    public ReadOnlySpan<byte> Key { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _data.Key; }
    public ReadOnlySpan<byte> Value { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _data.Value; }

    public void SeekToFirst()
    {
        _index.SeekToFirst();
        if (!LoadCurrentBlock()) return;
        _data.SeekToFirst();
        FindKeyForward();
    }

    public void SeekToLast()
    {
        _index.SeekToLast();
        if (!LoadCurrentBlock()) return;
        _data.SeekToLast();
        FindKeyBackward();
    }

    /// <summary>Positions at the first entry with internal key &gt;= <paramref name="target"/>.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target, backward: false))
        {
            ReleaseBlock();
            return;
        }
        _index.Seek(target);
        if (!LoadCurrentBlock()) return;
        _data.Seek(target);
        FindKeyForward();
    }

    /// <summary>Positions at the last entry with internal key &lt;= <paramref name="target"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target, backward: true))
        {
            ReleaseBlock();
            return;
        }
        _index.Seek(target);
        if (!_index.Valid) _index.SeekToLast();
        if (!LoadCurrentBlock()) return;
        _data.SeekForPrev(target);
        FindKeyBackward();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void Next()
    {
        System.Diagnostics.Debug.Assert(Valid);
        _data.Next();
        FindKeyForward();
    }

    public void Prev()
    {
        System.Diagnostics.Debug.Assert(Valid);
        _data.Prev();
        FindKeyBackward();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void FindKeyForward()
    {
        while (!_data.Valid)
        {
            _index.Next();
            if (!LoadCurrentBlock()) return;
            _data.SeekToFirst();
        }
    }

    private void FindKeyBackward()
    {
        while (!_data.Valid)
        {
            _index.Prev();
            if (!LoadCurrentBlock()) return;
            _data.SeekToLast();
        }
    }

    /// <summary>Loads the data block at the index position (keeps the current one if unchanged). False when the index is exhausted.</summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool LoadCurrentBlock()
    {
        if (!_index.Valid)
        {
            ReleaseBlock();
            return false;
        }
        BlockHandle h = _index.Handle;
        if (!_block.IsEmpty && h == _blockHandle) return true;
        ReleaseBlock();
        SetBlock(_reader.GetBlockRef(h, BlockKind.Data, _flags, _prefetch), h);
        return true;
    }

    private void SetBlock(BlockRef b, BlockHandle h)
    {
        _block = b;
        _blockHandle = h;
        _data.InitializeData(b.Block!, _reader.InternalComparator, _reader.GlobalSequenceNumber, _reader.PadTimestampSize);
    }

    private void ReleaseBlock()
    {
        _data.Clear();
        if (_block.IsEmpty) return;
        Db.Iterators.PinnedIteratorsManager? mgr = _pinMgr;
        if (mgr is not null && mgr.IsPinning)
        {
            // block_iter_.DelegateCleanupsTo(pinned_iters_mgr_): values handed out stay valid until the manager releases.
            mgr.PinBlock(_block);
            _block = default;
            return;
        }
        _block.Release();
    }

    /// <summary>Port of SetPinnedItersMgr: blocks left while <paramref name="manager"/> is pinning are handed over to it.</summary>
    internal void SetPinnedItersMgr(Db.Iterators.PinnedIteratorsManager? manager) => _pinMgr = manager;

    /// <summary>
    /// Port of IsValuePinned (pinned_iters_mgr_ && PinningEnabled() && block_iter_.IsValuePinned()): the current value
    /// as a slice of the data block, which the manager keeps alive once the iterator moves on.
    /// </summary>
    internal bool TryGetPinnedValue(out Db.Iterators.PinnedSlice value)
    {
        if (_pinMgr is { IsPinning: true } && Valid)
        {
            value = _data.PinnedValue;
            return true;
        }
        value = default;
        return false;
    }

    // ---------------------------------------------------------------------------------------------
    // Async variants: identical positioning, but data block reads are awaited.

    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) =>
        ThenPositionAsync(_index.SeekToFirstAsync(cancellationToken), Op.First, default, backward: false, cancellationToken);

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target.Span, backward: false))
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        return ThenPositionAsync(_index.SeekAsync(target, cancellationToken), Op.Seek, target, backward: false, cancellationToken);
    }

    /// <summary>Advances; completes synchronously unless the next data block (or index partition) must be read from the file.</summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        _data.Next();
        if (_data.Valid) return ValueTask.CompletedTask;
        return ThenPositionAsync(_index.NextAsync(cancellationToken), Op.First, default, backward: false, cancellationToken);
    }

    private enum Op : byte { First, Seek }

    /// <summary>Continues with <see cref="PositionAsync"/> / <see cref="PositionBackwardAsync"/> once the index move completed.</summary>
    private ValueTask ThenPositionAsync(ValueTask indexMove, Op op, ReadOnlyMemory<byte> target, bool backward, CancellationToken ct)
    {
        if (!indexMove.IsCompletedSuccessfully) return ThenPositionSlowAsync(indexMove, op, target, backward, ct);
        indexMove.GetAwaiter().GetResult();
        return backward ? PositionBackwardAsync(op, target, ct) : PositionAsync(op, target, ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ThenPositionSlowAsync(ValueTask indexMove, Op op, ReadOnlyMemory<byte> target, bool backward, CancellationToken ct)
    {
        await indexMove.ConfigureAwait(false);
        if (backward) await PositionBackwardAsync(op, target, ct).ConfigureAwait(false);
        else await PositionAsync(op, target, ct).ConfigureAwait(false);
    }

    private ValueTask PositionAsync(Op op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        while (true)
        {
            if (!_index.Valid)
            {
                ReleaseBlock();
                return ValueTask.CompletedTask;
            }
            BlockHandle h = _index.Handle;
            if (_block.IsEmpty || h != _blockHandle)
            {
                ReleaseBlock();
                ValueTask<BlockRef> pending = _reader.GetBlockRefAsync(h, BlockKind.Data, _flags, ct);
                if (!pending.IsCompletedSuccessfully) return PositionSlowAsync(pending, h, op, target, ct);
                SetBlock(pending.Result, h);
            }
            if (op == Op.Seek) _data.Seek(target.Span);
            else _data.SeekToFirst();
            if (_data.Valid) return ValueTask.CompletedTask;
            op = Op.First;
            ValueTask next = _index.NextAsync(ct);
            if (!next.IsCompletedSuccessfully) return ThenPositionSlowAsync(next, Op.First, default, backward: false, ct);
            next.GetAwaiter().GetResult();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionSlowAsync(ValueTask<BlockRef> pending, BlockHandle h, Op op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        SetBlock(await pending.ConfigureAwait(false), h);
        if (op == Op.Seek) _data.Seek(target.Span);
        else _data.SeekToFirst();
        if (_data.Valid) return;
        await _index.NextAsync(ct).ConfigureAwait(false);
        await PositionAsync(Op.First, default, ct).ConfigureAwait(false);
    }

    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default) =>
        ThenPositionAsync(_index.SeekToLastAsync(cancellationToken), Op.First, default, backward: true, cancellationToken);

    /// <summary>Async <see cref="SeekForPrev"/>: completes synchronously unless a data block or index partition must be read from the file.</summary>
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target.Span, backward: true))
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        ValueTask seek = _index.SeekAsync(target, cancellationToken);
        if (!seek.IsCompletedSuccessfully) return SeekForPrevSlowAsync(seek, target, cancellationToken);
        seek.GetAwaiter().GetResult();
        if (!_index.Valid) return ThenPositionAsync(_index.SeekToLastAsync(cancellationToken), Op.Seek, target, backward: true, cancellationToken);
        return PositionBackwardAsync(Op.Seek, target, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SeekForPrevSlowAsync(ValueTask seek, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        await seek.ConfigureAwait(false);
        if (!_index.Valid) await _index.SeekToLastAsync(ct).ConfigureAwait(false);
        await PositionBackwardAsync(Op.Seek, target, ct).ConfigureAwait(false);
    }

    /// <summary>Async <see cref="Prev"/>: completes synchronously unless the previous data block (or index partition) must be read from the file.</summary>
    public ValueTask PrevAsync(CancellationToken cancellationToken = default)
    {
        System.Diagnostics.Debug.Assert(Valid);
        _data.Prev();
        if (_data.Valid) return ValueTask.CompletedTask;
        return ThenPositionAsync(_index.PrevAsync(cancellationToken), Op.First, default, backward: true, cancellationToken);
    }

    /// <summary>
    /// Backward counterpart of <see cref="PositionAsync"/> (LoadCurrentBlock + FindKeyBackward): loads the block at the
    /// index position, positions in it (SeekForPrev for <see cref="Op.Seek"/>, else SeekToLast) and moves to earlier
    /// blocks while it is empty.
    /// </summary>
    private ValueTask PositionBackwardAsync(Op op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        while (true)
        {
            if (!_index.Valid)
            {
                ReleaseBlock();
                return ValueTask.CompletedTask;
            }
            BlockHandle h = _index.Handle;
            if (_block.IsEmpty || h != _blockHandle)
            {
                ReleaseBlock();
                ValueTask<BlockRef> pending = _reader.GetBlockRefAsync(h, BlockKind.Data, _flags, ct);
                if (!pending.IsCompletedSuccessfully) return PositionBackwardSlowAsync(pending, h, op, target, ct);
                SetBlock(pending.Result, h);
            }
            if (op == Op.Seek) _data.SeekForPrev(target.Span);
            else _data.SeekToLast();
            if (_data.Valid) return ValueTask.CompletedTask;
            op = Op.First;
            ValueTask prev = _index.PrevAsync(ct);
            if (!prev.IsCompletedSuccessfully) return ThenPositionSlowAsync(prev, Op.First, default, backward: true, ct);
            prev.GetAwaiter().GetResult();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionBackwardSlowAsync(ValueTask<BlockRef> pending, BlockHandle h, Op op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        SetBlock(await pending.ConfigureAwait(false), h);
        if (op == Op.Seek) _data.SeekForPrev(target.Span);
        else _data.SeekToLast();
        if (_data.Valid) return;
        await _index.PrevAsync(ct).ConfigureAwait(false);
        await PositionBackwardAsync(Op.First, default, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Re-points this iterator (invalid afterwards) at another table with the same read flags, reusing its index and
    /// block iterators and their key buffers: a level iterator moving between files does not allocate.
    /// </summary>
    /// <summary>Releases the current data block and the index blocks (the iterator is invalid until <see cref="Retarget"/>).</summary>
    internal void Detach()
    {
        ReleaseBlock();
        _index.Reset();
        _prefetch?.Reset(null);
    }

    /// <summary>
    /// Reads data blocks that are not cached through a readahead buffer of <paramref name="readaheadSize"/> bytes that
    /// prefetches the following chunk asynchronously (BlockPrefetcher with compaction_readahead_size and async_io).
    /// For sequential scans of whole files (compaction inputs); the sync positioning paths use it, the async ones read
    /// blocks directly. Idempotent.
    /// </summary>
    internal void EnableReadahead(int readaheadSize)
    {
        if (_prefetch is not null || readaheadSize <= 0) return;
        _prefetch = new Env.AsyncPrefetchBuffer(readaheadSize);
        _prefetch.Reset(_reader.File);
    }

    /// <summary>The readahead buffer (tests / diagnostics), or null.</summary>
    internal Env.AsyncPrefetchBuffer? Readahead => _prefetch;

    internal void Retarget(BlockBasedTableReader reader, in TableIteratorPrefixOptions prefixOptions)
    {
        ReleaseBlock();
        _reader = reader;
        _prefix = prefixOptions;
        _index.Init(reader, _flags);
        _prefetch?.Reset(reader.File);
    }

    public void Dispose()
    {
        ReleaseBlock();
        _index.Dispose();
        _data.Dispose();
        _prefetch?.Dispose();
    }
}
