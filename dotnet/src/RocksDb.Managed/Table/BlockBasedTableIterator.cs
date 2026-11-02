using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table;

/// <summary>
/// Iterator over the table's index yielding data block handles; transparently handles a two-level (partitioned)
/// index. Port of the index-iteration parts of BinarySearchIndexReader / PartitionedIndexIterator
/// (table/block_based/partitioned_index_iterator.cc). Index partitions are loaded synchronously.
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
        reader.InitIndexIter(_top, _topRef.Block!);
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
        _reader.InitIndexIter(_part, _partRef.Block!);
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
/// The async variants only await when a data block has to be read from the file (cache hits complete
/// synchronously). Not thread-safe; dispose to release the current block.
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
        SetBlock(_reader.GetBlockRef(h, BlockKind.Data, _flags), h);
        return true;
    }

    private void SetBlock(BlockRef b, BlockHandle h)
    {
        _block = b;
        _blockHandle = h;
        _data.InitializeData(b.Block!, _reader.InternalComparator, _reader.GlobalSequenceNumber);
    }

    private void ReleaseBlock()
    {
        _data.Clear();
        if (!_block.IsEmpty) _block.Release();
    }

    // ---------------------------------------------------------------------------------------------
    // Async variants: identical positioning, but data block reads are awaited.

    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        _index.SeekToFirst();
        return PositionAsync(Op.First, default, cancellationToken);
    }

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target.Span, backward: false))
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        _index.Seek(target.Span);

        return PositionAsync(Op.Seek, target, cancellationToken);
    }

    /// <summary>Advances; completes synchronously unless the next data block must be read from the file.</summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        _data.Next();
        if (_data.Valid) return ValueTask.CompletedTask;
        _index.Next();
        return PositionAsync(Op.First, default, cancellationToken);
    }

    private enum Op : byte { First, Seek }

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
            _index.Next();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask PositionSlowAsync(ValueTask<BlockRef> pending, BlockHandle h, Op op, ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        SetBlock(await pending.ConfigureAwait(false), h);
        if (op == Op.Seek) _data.Seek(target.Span);
        else _data.SeekToFirst();
        if (_data.Valid) return;
        _index.Next();
        await PositionAsync(Op.First, default, ct).ConfigureAwait(false);
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
    }

    internal void Retarget(BlockBasedTableReader reader, in TableIteratorPrefixOptions prefixOptions)
    {
        ReleaseBlock();
        _reader = reader;
        _prefix = prefixOptions;
        _index.Init(reader, _flags);
    }

    public void Dispose()
    {
        ReleaseBlock();
        _index.Dispose();
        _data.Dispose();
    }
}
