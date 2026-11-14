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
    /// <summary>Offset of the partition behind <see cref="_partRef"/> (prev_block_offset_ of PartitionedIndexIterator).</summary>
    private ulong _partOffset;
    private bool _partitioned;
    private TableReadFlags _flags;
    // User-defined index routing (IndexFactoryReaderWrapper / IndexFactoryIteratorWrapper,
    // table/block_based/user_defined_index_wrapper.h): when set, every move goes through the custom index.
    private UserDefinedIndexIterator? _udi;
    private bool _udiValid;
    private byte[] _udiKey = new byte[64];
    private int _udiKeyLength;

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
        _udi = reader.NewUserDefinedIndexIterator(flags);
        if (_udi is not null)
        {
            _udiValid = false;
            _partitioned = false;
            return;
        }
        _partitioned = reader.IsPartitionedIndex;
        _topRef = reader.GetIndexBlockRef(flags);
        reader.InitIndexIter(_top, _topRef.Block!, isPartition: false);
    }

    /// <summary>
    /// UpdateValidAndKey of IndexFactoryIteratorWrapper: the separator as the index key form of the table (internal key
    /// with seq 0 / kTypeValue when the standard index keys include the sequence, else the user key).
    /// </summary>
    private void UdiUpdate(bool valid)
    {
        _udiValid = valid;
        if (!valid) return;
        ReadOnlySpan<byte> sep = _udi!.Key;
        int n = _reader.IndexKeyIncludesSeq ? sep.Length + Db.DbFormat.InternalKeyFooterSize : sep.Length;
        if (_udiKey.Length < n) _udiKey = new byte[Math.Max(n, _udiKey.Length * 2)];
        if (_reader.IndexKeyIncludesSeq) Db.DbFormat.AppendInternalKey(_udiKey, sep, 0, Db.EntryType.Value);
        else sep.CopyTo(_udiKey);
        _udiKeyLength = n;
    }

    private void UdiSeek(ReadOnlySpan<byte> target)
    {
        if (target.Length < Db.DbFormat.InternalKeyFooterSize) ThrowHelper.Corruption("Invalid internal key (too short)");
        UdiUpdate(_udi!.Seek(Db.DbFormat.ExtractUserKey(target), Db.DbFormat.ExtractFooter(target)));
    }

    public void Reset()
    {
        UnloadPartition();
        ReleaseTop();
        _udi?.Dispose();
        _udi = null;
        _udiValid = false;
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
        get => _udi is not null ? _udiValid : _partitioned ? !_partRef.IsEmpty && _part.Valid : _top.Valid;
    }

    public BlockHandle Handle => _udi is not null ? _udi.Value : _partitioned ? _part.BlockHandle : _top.BlockHandle;
    public ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        get => _udi is not null ? _udiKey.AsSpan(0, _udiKeyLength) : _partitioned ? _part.Key : _top.Key;
    }

    /// <summary>
    /// IndexBlockIter::user_key() / PartitionedIndexIterator::user_key(): the user key of the separator (the index key
    /// without its sequence footer when the index keys include it).
    /// </summary>
    public ReadOnlySpan<byte> UserKey
    {
        get
        {
            ReadOnlySpan<byte> k = Key;
            return _reader.IndexKeyIncludesSeq ? k[..^Db.DbFormat.InternalKeyFooterSize] : k;
        }
    }

    public void Seek(ReadOnlySpan<byte> target)
    {
        if (_udi is not null) { UdiSeek(target); return; }
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
        if (_udi is not null) { UdiSeek(target); return; }
        if (!_partitioned && _reader.TryGetPrefixBlocks(target, out ReadOnlySpan<uint> blocks))
        {
            _top.PrefixSeek(target, blocks);
            return;
        }
        Seek(target);
    }

    public void SeekToFirst()
    {
        if (_udi is not null) { UdiUpdate(_udi.SeekToFirst()); return; }
        _top.SeekToFirst();
        if (!_partitioned) return;
        if (!_top.Valid) { UnloadPartition(); return; }
        LoadPartition();
        _part.SeekToFirst();
        SkipEmptyForward();
    }

    public void SeekToLast()
    {
        if (_udi is not null) { UdiUpdate(_udi.SeekToLast()); return; }
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
        if (_udi is not null) { UdiUpdate(_udi.Next()); return; }
        if (!_partitioned) { _top.Next(); return; }
        _part.Next();
        SkipEmptyForward();
    }

    public void Prev()
    {
        if (_udi is not null) { UdiUpdate(_udi.Prev()); return; }
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

    /// <summary>
    /// PartitionedIndexIterator::InitPartitionedIndexBlock: loads the partition the top level points to, unless it is
    /// the one already loaded (no second lookup for a move that stays in the same partition).
    /// </summary>
    private void LoadPartition()
    {
        BlockHandle handle = _top.BlockHandle;
        if (!_partRef.IsEmpty && handle.Offset == _partOffset) return;
        UnloadPartition();
        _partRef = _reader.GetIndexPartitionRef(handle, _flags);
        _partOffset = handle.Offset;
        _reader.InitIndexIter(_part, _partRef.Block!, isPartition: true);
    }

    // ---- async moves: partitions read with async I/O ----

    /// <summary>How to position inside a newly loaded partition.</summary>
    private enum PartOp : byte { Seek, First, Last }

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        if (_udi is not null) { UdiSeek(target.Span); return ValueTask.CompletedTask; }
        _top.Seek(target.Span);
        return AfterTopMoveAsync(PartOp.Seek, target, ct);
    }

    /// <summary>Async <see cref="SeekForGet"/> (the hash index prefix seek is in memory).</summary>
    public ValueTask SeekForGetAsync(ReadOnlyMemory<byte> target, CancellationToken ct)
    {
        if (_udi is not null) { UdiSeek(target.Span); return ValueTask.CompletedTask; }
        if (!_partitioned && _reader.TryGetPrefixBlocks(target.Span, out ReadOnlySpan<uint> blocks))
        {
            _top.PrefixSeek(target.Span, blocks);
            return ValueTask.CompletedTask;
        }
        return SeekAsync(target, ct);
    }

    public ValueTask SeekToFirstAsync(CancellationToken ct)
    {
        if (_udi is not null) { SeekToFirst(); return ValueTask.CompletedTask; }
        _top.SeekToFirst();
        return AfterTopMoveAsync(PartOp.First, default, ct);
    }

    public ValueTask SeekToLastAsync(CancellationToken ct)
    {
        if (_udi is not null) { SeekToLast(); return ValueTask.CompletedTask; }
        _top.SeekToLast();
        return AfterTopMoveAsync(PartOp.Last, default, ct);
    }

    public ValueTask NextAsync(CancellationToken ct)
    {
        if (_udi is not null) { Next(); return ValueTask.CompletedTask; }
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
        if (_udi is not null) { Prev(); return ValueTask.CompletedTask; }
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
        BlockHandle handle = _top.BlockHandle;
        if (!_partRef.IsEmpty && handle.Offset == _partOffset)
        {
            // InitPartitionedIndexBlock: the partition is already loaded.
            PositionInLoadedPartition(op, target);
            return SkipEmptyAsync(op != PartOp.Last, ct);
        }
        UnloadPartition();
        _partOffset = handle.Offset;
        ValueTask<BlockRef> pending = _reader.GetIndexPartitionRefAsync(handle, _flags, ct);
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
        PositionInLoadedPartition(op, target);
    }

    private void PositionInLoadedPartition(PartOp op, ReadOnlyMemory<byte> target)
    {
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
/// (table/block_based/block_based_table_iterator.cc), including its iterate_upper_bound checks
/// (<see cref="SetUpperBound"/>: a data block entirely past the bound is not read, and the iterator reports
/// <see cref="UpperBoundCheckResult"/>) and ReadOptions::auto_readahead_size (<see cref="SetReadaheadOptions"/>: a
/// forward scan's readahead buffer is trimmed to the blocks within the bound / the seek key's prefix that are not
/// cached). With prefix options
/// (<see cref="TableIteratorPrefixOptions.CheckFilter"/>), Seek and SeekForPrev first consult the prefix filter
/// (CheckPrefixMayMatch) and leave the iterator invalid without reading any data block when it excludes the prefix.
/// <para>
/// Keys/values are spans into pinned block buffers (or the iterator's key buffer) valid until the next move.
/// The async variants (forward and reverse) only await when a data block or an index partition has to be read from the
/// file (cache hits complete synchronously). Not thread-safe; dispose to release the current block.
/// </para>
/// </summary>
public sealed class BlockBasedTableIterator : IDisposable, Env.IReadaheadSizeCallback
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

    /// <summary>block_based_table_iterator.h BlockUpperBound: where iterate_upper_bound lies relative to the current data block.</summary>
    private enum BlockUpperBound : byte
    {
        /// <summary>kUnknown: not checked (no bound, or no current data block).</summary>
        Unknown,
        /// <summary>kUpperBoundInCurBlock: the bound is &lt;= the block's index key, so the next block is out of bound.</summary>
        InCurBlock,
        /// <summary>kUpperBoundBeyondCurBlock: every key of the block is below the bound.</summary>
        BeyondCurBlock,
    }

    // ReadOptions::iterate_upper_bound (read_options_.iterate_upper_bound), block_upper_bound_check_ and is_out_of_bound_.
    private ReadOnlyMemory<byte> _upperBound;
    private bool _hasUpperBound;
    // The table's comparator and its timestamp size (cached: both are virtual reads).
    private Db.InternalKeyComparator _icmp;
    private int _tsSize;
    private bool _isOutOfBound;
    private BlockUpperBound _blockUpperBoundCheck;

    /// <summary>How data blocks that are not cached are read (BlockPrefetcher).</summary>
    private enum ReadaheadMode : byte
    {
        /// <summary>Implicit auto readahead after sequential reads (user iterators without readahead_size).</summary>
        Auto,
        /// <summary>A fixed readahead buffer (ReadOptions::readahead_size).</summary>
        Fixed,
        /// <summary>
        /// Compaction input readahead (compaction_readahead_size): an OS prefetch of the following bytes, or the readahead
        /// buffer when the file does not support Prefetch.
        /// </summary>
        Compaction,
        /// <summary>No readahead (compaction inputs with compaction_readahead_size = 0).</summary>
        None,
    }

    private ReadaheadMode _readaheadMode;
    private int _compactionReadahead;
    // BlockPrefetcher's implicit auto readahead state: num_file_reads_, prev_offset_ / prev_len_, readahead_size_ and
    // readahead_limit_ (OS prefetch), and whether the readahead buffer is in use (fallback when the file does not
    // support Prefetch).
    private ulong _numFileReads;
    private ulong _prevOffset;
    private ulong _prevLen;
    private ulong _readaheadSize;
    private ulong _readaheadLimit;
    private bool _autoPrefetching;

    // BlockBasedTableIterator::SeekStatState (block_based_table_iterator.h): kNone, kDataBlockReadSinceLastSeek,
    // kReportOnUseful and kFilterUsed, for the {NON_,}LAST_LEVEL_SEEK_DATA* tickers.
    private const byte SeekStatDataBlockRead = 1;
    private const byte SeekStatReportOnUseful = 2;
    private const byte SeekStatFilterUsed = 4;
    private byte _seekStat;
    /// <summary>The iterator reads a compaction input (lookup_context_.caller == kCompaction): no seek tickers.</summary>
    private bool _forCompaction;

    // ReadOptions::auto_readahead_size (block_based_table_iterator.h "APIs relevant to auto tuning of readahead_size"):
    // read_options_.auto_readahead_size / prefix_same_as_start, prefix_extractor_, readahead_cache_lookup_,
    // is_index_out_of_bound_, is_index_at_curr_block_, direction_ (kBackward disables the lookup until the next file),
    // block_handles_ (a ring buffer, power-of-two capacity) and seek_key_prefix_for_readahead_trimming_.
    private bool _autoReadaheadSize;
    private bool _prefixSameAsStart;
    private SliceTransform? _prefixExtractor;
    private bool _readaheadCacheLookup;
    private bool _isIndexOutOfBound;
    private bool _isIndexAtCurrBlock = true;
    private bool _backward;
    /// <summary>The readahead buffer's callback was bound (CreateFilePrefetchBufferIfNotExists runs once per file).</summary>
    private bool _readaheadCallbackBound;
    private BlockHandleInfo[] _blockHandles = [];
    private int _blockHandlesHead;
    private int _blockHandlesCount;
    private byte[] _seekKeyPrefix = [];
    private int _seekKeyPrefixLength;
    /// <summary>The current key, copied for the index reseek of <see cref="PrevAsync"/>.</summary>
    private byte[] _reseekKey = [];

    /// <summary>
    /// BlockHandleInfo: a data block the readahead lookup passed, with its block cache entry pinned when it was cached
    /// (moved to the iterator when the block becomes current).
    /// </summary>
    private struct BlockHandleInfo
    {
        public BlockHandle Handle;
        public BlockRef Entry;
        public bool IsCacheHit;
    }

    internal BlockBasedTableIterator(BlockBasedTableReader reader, TableReadFlags flags, in TableIteratorPrefixOptions prefixOptions)
    {
        _reader = reader;
        _flags = flags;
        _prefix = prefixOptions;
        _icmp = reader.InternalComparator;
        _tsSize = _icmp.UserComparator.TimestampSize;
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
        // skip_filters (optimize_filters_for_hits on the last non-empty level): BlockBasedTable::NewIterator check_filter.
        if (!_prefix.CheckFilter || (_flags & TableReadFlags.SkipFilters) != 0) return true;
        if (!_reader.PrefixRangeMayMatch(target, _prefix, _flags, out bool filterChecked))
        {
            _reader.RecordSeekFilterResult(filtered: true);
            return false;
        }
        if (filterChecked)
        {
            _reader.RecordSeekFilterResult(filtered: false);
            _seekStat = SeekStatFilterUsed; // SeekImpl / SeekForPrev: seek_stat_state_ = kFilterUsed
        }
        return true;
    }

    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => !_isOutOfBound && !_block.IsEmpty && _data.Valid;
    }

    /// <summary>Port of BlockBasedTableIterator::UpperBoundCheckResult (block_based_table_iterator.h).</summary>
    public Db.Iterators.IterBoundCheck UpperBoundCheckResult =>
        _isOutOfBound ? Db.Iterators.IterBoundCheck.OutOfBound
        : _blockUpperBoundCheck == BlockUpperBound.BeyondCurBlock ? Db.Iterators.IterBoundCheck.Inbound
        : Db.Iterators.IterBoundCheck.Unknown;

    /// <summary>
    /// Sets ReadOptions::iterate_upper_bound (exclusive user key; null: none), honoured from the next positioning call.
    /// The memory must stay unchanged while the iterator uses it (upstream keeps a pointer to the read options' slice).
    /// </summary>
    internal void SetUpperBound(ReadOnlyMemory<byte>? upperBound)
    {
        _hasUpperBound = upperBound.HasValue;
        _upperBound = upperBound.GetValueOrDefault();
        // The block check was made against the previous bound; every positioning call recomputes it (upstream's
        // next_block_is_out_of_bound also requires a bound, so a stale state there is never read).
        _blockUpperBoundCheck = BlockUpperBound.Unknown;
    }

    /// <summary>
    /// Port of CheckOutOfBound: once the bound may lie in the current block, compares the current key with it
    /// (is_out_of_bound_ makes the iterator invalid, so a merging iterator leaves it out of its heap). Called after the
    /// forward seeks, like upstream, but not on every <see cref="Next"/> within a block (upstream does): the DB iterator
    /// compares each key with the bound itself, and does not consult UpperBoundCheckResult to skip that comparison
    /// (upstream's IteratorWrapper caches the result of NextAndGetResult; here it would be a chain of virtual calls per
    /// key, measured slower than the comparison it saves), so a second comparison per key would only cost time (5-7% on
    /// short bounded scans, BoundedScanBenchmarks) without changing what any caller observes: a table iterator left on a
    /// key at or past the bound is never stepped again before the next seek. The block-level check, which saves the
    /// read of the data block after the bound, is done at every block transition (<see cref="FindKeyForward"/>).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckOutOfBound()
    {
        if (_hasUpperBound && _blockUpperBoundCheck != BlockUpperBound.BeyondCurBlock && Valid)
            _isOutOfBound = CompareUpperBound(_data.Key[..^Db.DbFormat.InternalKeyFooterSize]) <= 0;
    }

    /// <summary>
    /// Port of CheckDataBlockWithinUpperBound: whether every key of the current block (all &lt;= its index key) is below
    /// the bound. Called whenever a data block becomes current, and on a reseek that stays in the current block (the
    /// bound may have changed since).
    /// </summary>
    private void CheckDataBlockWithinUpperBound()
    {
        // The readahead lookup may have moved the index past the current block (IsIndexAtCurr()).
        if (!_isIndexAtCurrBlock || !_hasUpperBound || _block.IsEmpty) return;
        _blockUpperBoundCheck = CompareUpperBound(_index.UserKey) > 0 ? BlockUpperBound.BeyondCurBlock : BlockUpperBound.InCurBlock;
    }

    /// <summary>
    /// user_comparator_.CompareWithoutTimestamp(iterate_upper_bound, false, <paramref name="userKey"/>, true): the bound
    /// carries no timestamp, the table's user keys do (if the comparator has them).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareUpperBound(ReadOnlySpan<byte> userKey)
    {
        return _tsSize == 0
            ? _icmp.CompareUserKey(_upperBound.Span, userKey)
            : _icmp.UserComparator.CompareWithoutTimestamp(_upperBound.Span, false, userKey, true);
    }

    /// <summary>Current internal key.</summary>
    public ReadOnlySpan<byte> Key { [MethodImpl(MethodImplOptions.AggressiveOptimization)] get => _data.Key; }
    public ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        get
        {
            if ((_seekStat & SeekStatReportOnUseful) != 0) ReportSeekDataUseful();
            return _data.Value;
        }
    }

    /// <summary>
    /// BlockBasedTableIterator::value(): the first value read after a seek that had to load a data block records
    /// {NON_,}LAST_LEVEL_SEEK_DATA_USEFUL_{FILTER_MATCH,NO_FILTER}.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ReportSeekDataUseful()
    {
        _reader.RecordSeekDataUseful((_seekStat & SeekStatFilterUsed) != 0);
        _seekStat = SeekStatDataBlockRead;
    }

    /// <summary>
    /// InitDataBlock / AsyncInitDataBlock: the first data block loaded since the last seek records
    /// {NON_,}LAST_LEVEL_SEEK_DATA (not for a compaction input, and nothing without a statistics object).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void RecordSeekDataBlockRead()
    {
        _seekStat |= SeekStatDataBlockRead;
        if (_forCompaction || !_reader.RecordSeekData()) return;
        _seekStat |= SeekStatReportOnUseful;
    }

    public void SeekToFirst()
    {
        // No seek key, so no prefix to trim the readahead against.
        _seekKeyPrefixLength = 0;
        BeginForwardSeek();
        _index.SeekToFirst();
        _isIndexAtCurrBlock = true;
        if (!LoadCurrentBlock()) return;
        _data.SeekToFirst();
        FindKeyForward();
        CheckOutOfBound();
    }

    public void SeekToLast()
    {
        BeginBackwardSeek();
        _index.SeekToLast();
        _isIndexAtCurrBlock = true;
        if (!LoadCurrentBlock()) return;
        _data.SeekToLast();
        FindKeyBackward();
    }

    /// <summary>Positions at the first entry with internal key &gt;= <paramref name="target"/>.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        SetSeekKeyPrefix(target);
        BeginForwardSeek();
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target, backward: false))
        {
            ReleaseBlock();
            return;
        }
        _index.Seek(target);
        _isIndexAtCurrBlock = true;
        if (!LoadCurrentBlock()) return;
        _data.Seek(target);
        FindKeyForward();
        CheckOutOfBound();
    }

    /// <summary>Positions at the last entry with internal key &lt;= <paramref name="target"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        BeginBackwardSeek();
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target, backward: true))
        {
            ReleaseBlock();
            return;
        }
        _index.Seek(target);
        _isIndexAtCurrBlock = true;
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
        // Upstream Next() also calls CheckOutOfBound(): not here. See CheckOutOfBound.
        FindKeyForward();
    }

    public void Prev()
    {
        System.Diagnostics.Debug.Assert(Valid);
        if (_readaheadCacheLookup && !_isIndexAtCurrBlock)
        {
            // The readahead lookup moved the index forward: reseek it to the current block with the current key.
            ResetBlockCacheLookupVar();
            _backward = true;
            _index.Seek(_data.Key);
            _isIndexAtCurrBlock = true;
            if (!_index.Valid)
            {
                ReleaseBlock();
                return;
            }
        }
        ResetBlockCacheLookupVar();
        _data.Prev();
        FindKeyBackward();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private void FindKeyForward()
    {
        if (!_data.Valid) FindBlockForward();
    }

    /// <summary>
    /// Port of FindBlockForward: moves to the next data block that has a key. With the readahead lookup, the next block
    /// is the next queued handle, or the one the index already points to once the queue is empty. A block is not read
    /// when the bound lies in the block just finished (next_block_is_out_of_bound) or the lookup found the next block
    /// past the bound / the seek key's prefix (is_index_out_of_bound_): then, unless the finished block was the file's
    /// last (its index key can be larger than the smallest key of the next file of the level), the iterator stops out of
    /// bound.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void FindBlockForward()
    {
        do
        {
            // block_upper_bound_check_ is kUnknown without a bound or a current block.
            bool nextBlockIsOutOfBound = _isIndexAtCurrBlock && _blockUpperBoundCheck == BlockUpperBound.InCurBlock;
            ReleaseBlock();
            // Advance to the next block handle, making it current.
            if (_blockHandlesCount != 0) PopFrontBlockHandle();
            if (_blockHandlesCount == 0)
            {
                // Without queued handles, the index is moved unless the lookup already left it on the next block; the
                // last block the lookup reached may be out of bound, which is decided here.
                if (_isIndexAtCurrBlock || _isIndexOutOfBound)
                {
                    _index.Next();
                    if (_isIndexOutOfBound)
                    {
                        nextBlockIsOutOfBound = true;
                        _isIndexOutOfBound = false;
                    }
                }
                else
                {
                    _isIndexAtCurrBlock = true;
                }
                if (nextBlockIsOutOfBound)
                {
                    _isOutOfBound = _index.Valid;
                    return;
                }
                if (!_index.Valid) return;
            }
            LoadCurrentBlock();
            _data.SeekToFirst();
        }
        while (!_data.Valid);
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

    /// <summary>
    /// Port of InitDataBlock: loads the data block at the index position, or the first queued handle of the readahead
    /// lookup (from its pinned cache entry, or read without a second cache lookup), keeping the current one if unchanged.
    /// False when the index is exhausted.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private bool LoadCurrentBlock()
    {
        BlockHandle h;
        if (_blockHandlesCount != 0)
        {
            h = _blockHandles[_blockHandlesHead].Handle;
        }
        else
        {
            if (!_index.Valid)
            {
                ReleaseBlock();
                return false;
            }
            h = _index.Handle;
        }
        if (!_block.IsEmpty && h == _blockHandle)
        {
            // A reseek into the current block: the bound may have changed (SeekImpl's same_block case).
            CheckDataBlockWithinUpperBound();
            return true;
        }
        ReleaseBlock();
        if (_blockHandlesCount != 0)
        {
            LoadQueuedBlock(h);
            return true;
        }
        SetBlock(_reader.GetBlockRef(h, BlockKind.Data, _flags, PrefetchIfNeeded(h)), h);
        return true;
    }

    /// <summary>
    /// InitDataBlock for the first queued handle: a cached block from the entry the lookup pinned, otherwise read
    /// (through the readahead buffer) with use_block_cache_for_lookup = false, its miss having been counted by the lookup.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void LoadQueuedBlock(BlockHandle h)
    {
        ref BlockHandleInfo front = ref _blockHandles[_blockHandlesHead];
        if (front.IsCacheHit)
        {
            BlockRef entry = front.Entry;
            front.Entry = default;
            SetBlock(entry, h);
            return;
        }
        SetBlock(_reader.GetDataBlockRefWithoutCacheLookup(h, _flags, PrefetchIfNeeded(h)), h);
    }

    /// <summary>
    /// Port of BlockPrefetcher::PrefetchIfNeeded for the sync reads of a data block: the fixed readahead buffer
    /// (compaction or ReadOptions::readahead_size); or, in auto mode (implicit readahead), once
    /// num_file_reads_for_auto_readahead sequential blocks were read, an OS prefetch (<see cref="Env.IRandomAccessFile.Prefetch"/>,
    /// readahead(2)) of the block plus readahead_size_ bytes, starting at initial_auto_readahead_size and doubling up to
    /// max_auto_readahead_size, with no buffer and no copy; blocks inside the prefetched range (readahead_limit_) are
    /// read normally. Only when the file does not support Prefetch (direct I/O, other platforms) the readahead buffer
    /// takes over. A non-sequential block resets the state (ResetValues).
    /// </summary>
    private Env.AsyncPrefetchBuffer? PrefetchIfNeeded(BlockHandle h)
    {
        // BlockPrefetcher::PrefetchIfNeeded: no prefetching for kBlockCacheTier reads (no I/O allowed).
        if ((_flags & TableReadFlags.BlockCacheTier) != 0) return null;
        if (_readaheadMode != ReadaheadMode.Auto)
        {
            if (_readaheadMode == ReadaheadMode.Compaction) return PrefetchForCompaction(h);
            if (_prefetch is null) return null;
            BindReadaheadCallback(_prefetch);
            // FilePrefetchBuffer::TryReadFromCache: a block before the buffered range is read from the file directly.
            return _prefetch.IsBeforeBuffer((long)h.Offset) ? null : _prefetch;
        }
        TableReaderOptions o = _reader.Options;
        long max = o.MaxAutoReadaheadSize;
        long initial = Math.Min(o.InitialAutoReadaheadSize, max);
        // If max_auto_readahead_size is set to be 0 by user, no data will be prefetched.
        if (max <= 0 || initial <= 0) return null;
        ulong offset = h.Offset;
        ulong len = h.Size + BlockBasedTableConstants.BlockTrailerSize;
        // The OS already prefetched the block (readahead_limit_ is 0 when no Prefetch was issued).
        if (offset + len <= _readaheadLimit)
        {
            _prevOffset = offset;
            _prevLen = len;
            return null;
        }
        bool sequential = _prevLen == 0 || _prevOffset + _prevLen == offset;
        // UpdateReadPattern
        _prevOffset = offset;
        _prevLen = len;
        if (!sequential)
        {
            // ResetValues
            _numFileReads = 1;
            _readaheadSize = (ulong)initial;
            _readaheadLimit = 0;
            if (_autoPrefetching)
            {
                _autoPrefetching = false;
                _prefetch!.ResetReadahead();
            }
            return null;
        }
        if (_autoPrefetching) return _prefetch;
        // Implicit auto readahead, enabled once the number of reads reached num_file_reads_for_auto_readahead and
        // scans are sequential.
        if (++_numFileReads <= o.NumFileReadsForAutoReadahead) return null;
        if (_readaheadSize == 0) _readaheadSize = (ulong)initial;
        if (_readaheadSize > (ulong)max) _readaheadSize = (ulong)max;
        if (_reader.File.Prefetch((long)offset, (long)(len + _readaheadSize)))
        {
            _readaheadLimit = offset + len + _readaheadSize;
            // Keep exponentially increasing readahead size until max_auto_readahead_size.
            _readaheadSize = Math.Min((ulong)max, _readaheadSize * 2);
            return null;
        }
        // Prefetch not supported: fall back to the internal prefetch buffer.
        if (_prefetch is null) _prefetch = new Env.AsyncPrefetchBuffer((int)initial, (int)max);
        else _prefetch.ResetReadahead();
        _prefetch.Reset(_reader.File);
        _autoPrefetching = true;
        BindReadaheadCallback(_prefetch);
        return _prefetch;
    }

    /// <summary>
    /// The readaheadsize_cb_ BlockPrefetcher hands to the buffer it creates for a user scan
    /// (CreateFilePrefetchBufferIfNotExists, once per table iterator): this iterator's
    /// <see cref="Env.IReadaheadSizeCallback.TuneReadaheadSize"/> when readahead_cache_lookup_ is set at that point,
    /// else none.
    /// </summary>
    private void BindReadaheadCallback(Env.AsyncPrefetchBuffer prefetch)
    {
        if (_readaheadCallbackBound) return;
        _readaheadCallbackBound = true;
        prefetch.ReadaheadSizeCallback = _readaheadCacheLookup ? this : null;
        prefetch.Statistics = _reader.DbStatistics;
    }

    // ---------------------------------------------------------------------------------------------
    // ReadOptions::auto_readahead_size: BlockCacheLookupForReadAheadSize and its state.

    /// <summary>
    /// ReadOptions::auto_readahead_size and prefix_same_as_start, and the column family's prefix extractor (from
    /// <paramref name="readOptions"/>; null: the defaults, which never trim without a bound or prefix_same_as_start).
    /// Honoured from the next forward seek.
    /// </summary>
    internal void SetReadaheadOptions(ReadOptions? readOptions, SliceTransform? prefixExtractor)
    {
        _autoReadaheadSize = readOptions?.AutoReadaheadSize ?? true;
        _prefixSameAsStart = readOptions?.PrefixSameAsStart == true;
        _prefixExtractor = prefixExtractor;
    }

    /// <summary>
    /// The start of SeekImpl's first pass: forgets the current block when the readahead lookup moved the index past it
    /// (SavePrevIndexValue only records it with IsIndexAtCurr(), the lookup resets prev_block_offset_), resets the lookup
    /// and enables it for a forward scan with a block cache and iterate_upper_bound or prefix_same_as_start
    /// (autotune_readaheadsize).
    /// </summary>
    private void BeginForwardSeek()
    {
        if (!_isIndexAtCurrBlock) _blockHandle = default;
        ResetBlockCacheLookupVar();
        if (_autoReadaheadSize && (_hasUpperBound || _prefixSameAsStart) && !_backward && _reader.HasBlockCache) _readaheadCacheLookup = true;
        _seekStat = 0;
        _isOutOfBound = false;
    }

    /// <summary>SeekForPrev / SeekToLast: direction_ = kBackward, which keeps the lookup disabled for this file.</summary>
    private void BeginBackwardSeek()
    {
        _backward = true;
        if (!_isIndexAtCurrBlock) _blockHandle = default;
        ResetBlockCacheLookupVar();
        _seekStat = 0;
        _isOutOfBound = false;
    }

    /// <summary>
    /// SeekImpl: under prefix_same_as_start, the prefix of the seek key (empty when out of the extractor's domain) the
    /// readahead is trimmed to; without an extractor or prefix_same_as_start the previous one stays (unused then).
    /// </summary>
    private void SetSeekKeyPrefix(ReadOnlySpan<byte> target)
    {
        if (_prefixExtractor is null || !_prefixSameAsStart) return;
        SetSeekKeyPrefixSlow(target);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void SetSeekKeyPrefixSlow(ReadOnlySpan<byte> target)
    {
        ReadOnlySpan<byte> userKey = Db.DbFormat.ExtractUserKey(target);
        if (!_prefixExtractor!.InDomain(userKey))
        {
            _seekKeyPrefixLength = 0;
            return;
        }
        ReadOnlySpan<byte> prefix = _prefixExtractor.Transform(userKey);
        if (_seekKeyPrefix.Length < prefix.Length) _seekKeyPrefix = new byte[Math.Max(prefix.Length, 16)];
        prefix.CopyTo(_seekKeyPrefix);
        _seekKeyPrefixLength = prefix.Length;
    }

    /// <summary>Port of ResetBlockCacheLookupVar.</summary>
    private void ResetBlockCacheLookupVar()
    {
        _isIndexOutOfBound = false;
        _readaheadCacheLookup = false;
        if (_blockHandlesCount != 0) ClearBlockHandles();
    }

    /// <summary>ClearBlockHandles: drops the queued handles, unpinning the cache entries the lookup pinned.</summary>
    private void ClearBlockHandles()
    {
        BlockHandleInfo[] q = _blockHandles;
        int mask = q.Length - 1;
        for (int i = 0; i < _blockHandlesCount; i++)
        {
            ref BlockHandleInfo e = ref q[(_blockHandlesHead + i) & mask];
            if (!e.Entry.IsEmpty) e.Entry.Release();
            e = default;
        }
        _blockHandlesHead = 0;
        _blockHandlesCount = 0;
    }

    private void PopFrontBlockHandle()
    {
        ref BlockHandleInfo e = ref _blockHandles[_blockHandlesHead];
        if (!e.Entry.IsEmpty) e.Entry.Release();
        e = default;
        _blockHandlesHead = (_blockHandlesHead + 1) & (_blockHandles.Length - 1);
        _blockHandlesCount--;
    }

    private void PushBackBlockHandle(BlockHandle handle, BlockRef entry, bool isCacheHit)
    {
        if (_blockHandlesCount == _blockHandles.Length) GrowBlockHandles();
        ref BlockHandleInfo e = ref _blockHandles[(_blockHandlesHead + _blockHandlesCount) & (_blockHandles.Length - 1)];
        e.Handle = handle;
        e.Entry = entry;
        e.IsCacheHit = isCacheHit;
        _blockHandlesCount++;
    }

    private void GrowBlockHandles()
    {
        BlockHandleInfo[] old = _blockHandles;
        var q = new BlockHandleInfo[Math.Max(16, old.Length * 2)];
        for (int i = 0; i < _blockHandlesCount; i++) q[i] = old[(_blockHandlesHead + i) & (old.Length - 1)];
        _blockHandles = q;
        _blockHandlesHead = 0;
    }

    /// <summary>The <paramref name="fromBack"/>-th queued handle counted from the back (0: the last one).</summary>
    private ref BlockHandleInfo QueuedFromBack(int fromBack) =>
        ref _blockHandles[(_blockHandlesHead + _blockHandlesCount - 1 - fromBack) & (_blockHandles.Length - 1)];

    /// <summary>
    /// Port of IsNextBlockOutOfReadaheadBound: the index key of the block the index points to is at or past
    /// iterate_upper_bound, or (prefix_same_as_start) has another prefix than the seek key (or, out of the extractor's
    /// domain, sorts after it), so no key of the following blocks is in the scan.
    /// </summary>
    private bool IsNextBlockOutOfReadaheadBound()
    {
        ReadOnlySpan<byte> indexUserKey = _index.UserKey;
        if (_hasUpperBound && CompareUpperBound(indexUserKey) <= 0) return true;
        if (_prefixSameAsStart && _seekKeyPrefixLength != 0)
        {
            ReadOnlySpan<byte> seekPrefix = _seekKeyPrefix.AsSpan(0, _seekKeyPrefixLength);
            return _prefixExtractor!.InDomain(indexUserKey)
                ? !_prefixExtractor.Transform(indexUserKey).SequenceEqual(seekPrefix)
                : _icmp.UserComparator.CompareWithoutTimestamp(indexUserKey, true, seekPrefix, false) > 0;
        }
        return false;
    }

    /// <summary>
    /// Port of BlockCacheLookupForReadAheadSize (block_based_table_iterator.cc:857), the readahead buffer's
    /// readaheadsize_cb_: looks the data blocks of [<paramref name="startOffset"/>, <paramref name="endOffset"/>) up in
    /// the block cache, from the index position on, queueing every handle (cached ones with their entry pinned), and
    /// stops at the first block whose index key is out of the bound / prefix (is_index_out_of_bound_). The window is
    /// then trimmed to the first and last block that missed the cache. The index is left after the last queued block
    /// (is_index_at_curr_block_ = false), so it does not follow the current block until the queue is consumed.
    /// </summary>
    void Env.IReadaheadSizeCallback.TuneReadaheadSize(bool readCurrBlock, ref long startOffset, ref long endOffset)
    {
        // readahead_cache_lookup_ is false after a backward operation.
        if (!_readaheadCacheLookup) return;
        // The lookup may have moved the index to the end of the file while the current block is still consumed.
        if (!_index.Valid) return;
        const int footer = BlockBasedTableConstants.BlockTrailerSize;
        if (readCurrBlock && _blockHandlesCount == 0 && IsNextBlockOutOfReadaheadBound())
        {
            BlockHandle cur = _index.Handle;
            endOffset = (long)(cur.Offset + cur.Size) + footer;
            return;
        }
        long startUpdatedOffset = startOffset;
        long endUpdatedOffset = startUpdatedOffset;
        bool foundFirstMissBlock = false;
        int prevHandlesSize = _blockHandlesCount;

        // InitializeStartAndEndOffsets.
        if (readCurrBlock)
        {
            if (_blockHandlesCount == 0)
            {
                // Scenario 1: the block the caller misses and reads is the index's; it is queued without a lookup.
                BlockHandle cur = _index.Handle;
                endUpdatedOffset = (long)(cur.Offset + cur.Size) + footer;
                PushBackBlockHandle(cur, default, isCacheHit: false);
                _index.Next();
                _isIndexAtCurrBlock = false;
                foundFirstMissBlock = true;
            }
            else
            {
                // Scenario 2: the queued handles are read again (a failed or dropped read of the second buffer).
                foundFirstMissBlock = true;
                prevHandlesSize = 0;
                BlockHandle front = _blockHandles[_blockHandlesHead].Handle;
                BlockHandle back = QueuedFromBack(0).Handle;
                startUpdatedOffset = (long)front.Offset;
                endUpdatedOffset = (long)(back.Offset + back.Size) + footer;
            }
        }
        else if (_blockHandlesCount != 0)
        {
            // Scenario 3: a prefetch into the second buffer continues after the queued handles.
            BlockHandle back = QueuedFromBack(0).Handle;
            startUpdatedOffset = (long)(back.Offset + back.Size) + footer;
            endUpdatedOffset = startUpdatedOffset;
        }
        else
        {
            // Scenario 4: a prefetch with no queued handle (a reseek served from the block cache): from the index on.
            startUpdatedOffset = (long)_index.Handle.Offset;
            endUpdatedOffset = startUpdatedOffset;
        }

        while (_index.Valid && !_isIndexOutOfBound)
        {
            BlockHandle h = _index.Handle;
            // The block would exceed the window. The window can be smaller than one block when the readahead was trimmed
            // to the bound: the block is then read rather than skipped.
            if (startUpdatedOffset != endUpdatedOffset && endUpdatedOffset + (long)h.Size + footer > endOffset) break;
            endUpdatedOffset += footer + (long)h.Size;
            bool hit = _reader.LookupAndPinDataBlockInCache(h, out BlockRef entry);
            if (!foundFirstMissBlock && !hit)
            {
                foundFirstMissBlock = true;
                startUpdatedOffset = (long)h.Offset;
            }
            PushBackBlockHandle(h, entry, hit);
            // The current block's keys may be in bound, but if its index key is at or past the bound, the next one's are not.
            if (IsNextBlockOutOfReadaheadBound())
            {
                _isIndexOutOfBound = true;
                break;
            }
            _index.Next();
            _isIndexAtCurrBlock = false;
        }

        if (foundFirstMissBlock)
        {
            // Trim the end to the last block that missed, skipping the cached ones after it.
            int added = _blockHandlesCount - prevHandlesSize;
            int k = 0;
            while (k < added - 1 && QueuedFromBack(k).IsCacheHit && startUpdatedOffset != (long)QueuedFromBack(k).Handle.Offset) k++;
            BlockHandle last = QueuedFromBack(k).Handle;
            endUpdatedOffset = (long)(last.Offset + last.Size) + footer;
        }
        else
        {
            // Nothing to read: every block in the window is cached, or the bound was reached.
            endUpdatedOffset = startUpdatedOffset;
        }
        endOffset = endUpdatedOffset;
        startOffset = startUpdatedOffset;
        // ResetPreviousBlockOffset: a reseek forgets the current block while the index is not at it (BeginForwardSeek).
    }

    /// <summary>
    /// BlockPrefetcher::PrefetchIfNeeded for compaction (is_for_compaction): when the block is past the prefetched range
    /// (readahead_limit_), an OS prefetch of the block plus compaction_readahead_size bytes; blocks are then read one by
    /// one from the page cache. No buffer and no copy, like upstream with fs_prefetch_support. Only a file that does not
    /// support Prefetch (direct I/O, other platforms, in-memory files) is read through the readahead buffer.
    /// </summary>
    private Env.AsyncPrefetchBuffer? PrefetchForCompaction(BlockHandle h)
    {
        if (_prefetch is not null) return _prefetch;
        ulong offset = h.Offset;
        ulong len = h.Size + BlockBasedTableConstants.BlockTrailerSize;
        if (offset + len <= _readaheadLimit) return null;
        ulong size = (ulong)_compactionReadahead;
        if (_reader.File.Prefetch((long)offset, (long)(len + size)))
        {
            _readaheadLimit = offset + len + size;
            return null;
        }
        // Prefetch not supported: fall back to the internal prefetch buffer.
        _prefetch = new Env.AsyncPrefetchBuffer(_compactionReadahead);
        _prefetch.Reset(_reader.File);
        return _prefetch;
    }

    private void ResetAutoReadahead()
    {
        _numFileReads = 0;
        _prevOffset = 0;
        _prevLen = 0;
        _readaheadSize = 0;
        _readaheadLimit = 0;
        _autoPrefetching = false;
    }

    private void SetBlock(BlockRef b, BlockHandle h)
    {
        _block = b;
        _blockHandle = h;
        if ((_seekStat & SeekStatDataBlockRead) == 0) RecordSeekDataBlockRead();
        _data.InitializeData(b.Block!, _reader.InternalComparator, _reader.GlobalSequenceNumber, _reader.PadTimestampSize);
        CheckDataBlockWithinUpperBound();
    }

    /// <summary>Port of ResetDataIter: drops the current data block (block_upper_bound_check_ back to kUnknown).</summary>
    private void ReleaseBlock()
    {
        _blockUpperBoundCheck = BlockUpperBound.Unknown;
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
            if ((_seekStat & SeekStatReportOnUseful) != 0) ReportSeekDataUseful();
            value = _data.PinnedValue;
            return true;
        }
        value = default;
        return false;
    }

    // ---------------------------------------------------------------------------------------------
    // Async variants: identical positioning, but data block reads are awaited.

    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default)
    {
        _seekKeyPrefixLength = 0;
        BeginForwardSeek();
        _isIndexAtCurrBlock = true;
        return ThenPositionAsync(_index.SeekToFirstAsync(cancellationToken), Op.First, default, backward: false, cancellationToken);
    }

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        SetSeekKeyPrefix(target.Span);
        BeginForwardSeek();
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target.Span, backward: false))
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        _isIndexAtCurrBlock = true;
        return ThenPositionAsync(_index.SeekAsync(target, cancellationToken), Op.Seek, target, backward: false, cancellationToken);
    }

    /// <summary>Advances; completes synchronously unless the next data block (or index partition) must be read from the file.</summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default)
    {
        _data.Next();
        if (_data.Valid) return ValueTask.CompletedTask;
        if (_blockHandlesCount != 0 || !_isIndexAtCurrBlock || _isIndexOutOfBound)
        {
            // Readahead lookup state left by a synchronous move: the async moves read no readahead buffer and never queue
            // handles, so they consume the queue synchronously (cached blocks, or blocks in the readahead buffer).
            FindBlockForward();
            return ValueTask.CompletedTask;
        }
        if (_blockUpperBoundCheck == BlockUpperBound.InCurBlock) return StopBeforeNextBlockAsync(cancellationToken);
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
            else
            {
                CheckDataBlockWithinUpperBound();
            }
            if (op == Op.Seek) _data.Seek(target.Span);
            else _data.SeekToFirst();
            if (_data.Valid)
            {
                CheckOutOfBound();
                return ValueTask.CompletedTask;
            }
            if (_blockUpperBoundCheck == BlockUpperBound.InCurBlock) return StopBeforeNextBlockAsync(ct);
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
        if (_data.Valid)
        {
            CheckOutOfBound();
            return;
        }
        if (_blockUpperBoundCheck == BlockUpperBound.InCurBlock)
        {
            await StopBeforeNextBlockAsync(ct).ConfigureAwait(false);
            return;
        }
        await _index.NextAsync(ct).ConfigureAwait(false);
        await PositionAsync(Op.First, default, ct).ConfigureAwait(false);
    }

    /// <summary>Async <see cref="StopBeforeNextBlock"/> (the index may have to read its next partition).</summary>
    private ValueTask StopBeforeNextBlockAsync(CancellationToken ct)
    {
        ReleaseBlock();
        ValueTask next = _index.NextAsync(ct);
        if (!next.IsCompletedSuccessfully) return StopBeforeNextBlockSlowAsync(next);
        next.GetAwaiter().GetResult();
        _isOutOfBound = _index.Valid;
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask StopBeforeNextBlockSlowAsync(ValueTask next)
    {
        await next.ConfigureAwait(false);
        _isOutOfBound = _index.Valid;
    }

    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default)
    {
        BeginBackwardSeek();
        _isIndexAtCurrBlock = true;
        return ThenPositionAsync(_index.SeekToLastAsync(cancellationToken), Op.First, default, backward: true, cancellationToken);
    }

    /// <summary>Async <see cref="SeekForPrev"/>: completes synchronously unless a data block or index partition must be read from the file.</summary>
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default)
    {
        BeginBackwardSeek();
        if (_prefix.CheckFilter && !CheckPrefixMayMatch(target.Span, backward: true))
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        _isIndexAtCurrBlock = true;
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
        if (_readaheadCacheLookup && !_isIndexAtCurrBlock) return ReseekIndexThenPrevAsync(cancellationToken);
        ResetBlockCacheLookupVar();
        return PrevInBlockAsync(cancellationToken);
    }

    private ValueTask PrevInBlockAsync(CancellationToken ct)
    {
        _data.Prev();
        if (_data.Valid) return ValueTask.CompletedTask;
        return ThenPositionAsync(_index.PrevAsync(ct), Op.First, default, backward: true, ct);
    }

    /// <summary>
    /// <see cref="Prev"/>'s index reseek (the readahead lookup moved the index forward) with the index partition read
    /// asynchronously: the current key is copied, as the reseek may complete after an await.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private ValueTask ReseekIndexThenPrevAsync(CancellationToken ct)
    {
        ReadOnlySpan<byte> key = _data.Key;
        if (_reseekKey.Length < key.Length) _reseekKey = new byte[Math.Max(key.Length, 64)];
        key.CopyTo(_reseekKey);
        ResetBlockCacheLookupVar();
        _backward = true;
        ValueTask seek = _index.SeekAsync(_reseekKey.AsMemory(0, key.Length), ct);
        if (!seek.IsCompletedSuccessfully) return ReseekIndexThenPrevSlowAsync(seek, ct);
        seek.GetAwaiter().GetResult();
        return AfterIndexReseek(ct);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ReseekIndexThenPrevSlowAsync(ValueTask seek, CancellationToken ct)
    {
        await seek.ConfigureAwait(false);
        await AfterIndexReseek(ct).ConfigureAwait(false);
    }

    private ValueTask AfterIndexReseek(CancellationToken ct)
    {
        _isIndexAtCurrBlock = true;
        if (!_index.Valid)
        {
            ReleaseBlock();
            return ValueTask.CompletedTask;
        }
        return PrevInBlockAsync(ct);
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
            else
            {
                CheckDataBlockWithinUpperBound();
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
        ResetBlockCacheLookupVar();
        _isIndexAtCurrBlock = true;
        _isOutOfBound = false;
        _index.Reset();
        _prefetch?.Reset(null);
    }

    /// <summary>
    /// Compaction input readahead (BlockPrefetcher with compaction_readahead_size, is_for_compaction): data blocks that
    /// are not cached are preceded by an OS prefetch of the next <paramref name="readaheadSize"/> bytes
    /// (<see cref="Env.IRandomAccessFile.Prefetch"/>), or, where the file does not support it, read through a readahead
    /// buffer that prefetches the following chunk asynchronously. For sequential scans of whole files (compaction inputs,
    /// which never use auto readahead: 0 disables readahead); the sync positioning paths use it, the async ones read
    /// blocks directly. Idempotent.
    /// </summary>
    internal void EnableReadahead(int readaheadSize)
    {
        _forCompaction = true;
        if (readaheadSize <= 0)
        {
            if (_prefetch is null) _readaheadMode = ReadaheadMode.None;
            return;
        }
        if (_readaheadMode == ReadaheadMode.Compaction && _compactionReadahead == readaheadSize) return;
        _readaheadMode = ReadaheadMode.Compaction;
        _compactionReadahead = readaheadSize;
        _readaheadLimit = 0;
        _prefetch?.Dispose();
        _prefetch = null;
    }

    /// <summary>
    /// ReadOptions::readahead_size: an explicit readahead for a user iterator (BlockPrefetcher's "explicit user requested
    /// readahead": always the readahead buffer, initial = max readahead size); without it, iterators use the automatic
    /// readahead. Like upstream (num_buffers = async_io ? 2 : 1), one synchronous buffer
    /// (<see cref="Env.AsyncPrefetchBuffer.CreateSingleBuffer"/>), or with <paramref name="asyncIo"/> two buffers that
    /// read half the readahead each, the second asynchronously (TryReadFromCacheUntracked: readahead_size_ / 2).
    /// </summary>
    internal void SetUserReadahead(ulong readaheadSize, bool asyncIo)
    {
        if (readaheadSize == 0 || _readaheadMode == ReadaheadMode.Fixed) return;
        _readaheadMode = ReadaheadMode.Fixed;
        _prefetch?.Dispose();
        _readaheadCallbackBound = false;
        int size = (int)Math.Min(readaheadSize, 256UL << 20);
        _prefetch = asyncIo ? new Env.AsyncPrefetchBuffer(Math.Max(1, size / 2)) : Env.AsyncPrefetchBuffer.CreateSingleBuffer(size);
        _prefetch.Reset(_reader.File);
    }

    /// <summary>The readahead buffer (tests / diagnostics), or null.</summary>
    internal Env.AsyncPrefetchBuffer? Readahead => _prefetch;

    /// <remarks>The upper bound is cleared: the owner sets the bound of the new file's iterator (<see cref="SetUpperBound"/>).</remarks>
    internal void Retarget(BlockBasedTableReader reader, in TableIteratorPrefixOptions prefixOptions)
    {
        ReleaseBlock();
        // A new table iterator upstream (LevelIterator::NewFileIterator): forward, no lookup state, no seek key prefix,
        // and the readahead buffer's callback bound again at its first read.
        ResetBlockCacheLookupVar();
        _isIndexAtCurrBlock = true;
        _backward = false;
        _seekKeyPrefixLength = 0;
        _readaheadCallbackBound = false;
        _isOutOfBound = false;
        _hasUpperBound = false;
        _upperBound = default;
        _reader = reader;
        _icmp = reader.InternalComparator;
        _tsSize = _icmp.UserComparator.TimestampSize;
        _prefix = prefixOptions;
        _seekStat = 0;
        _index.Init(reader, _flags);
        _prefetch?.Reset(reader.File);
        ResetAutoReadahead();
    }

    public void Dispose()
    {
        ReleaseBlock();
        ResetBlockCacheLookupVar();
        _index.Dispose();
        _data.Dispose();
        _prefetch?.Dispose();
    }
}
