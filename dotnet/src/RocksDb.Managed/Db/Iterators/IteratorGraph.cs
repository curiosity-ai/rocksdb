using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// The re-targetable iterator stack of a DB iterator over one column family: a <see cref="DbIter"/> over a
/// <see cref="MergingIterator"/> of memtable iterators, one table iterator per L0 file and one <see cref="LevelIterator"/>
/// per non-empty level, built from a referenced super version like <see cref="DbIteratorFactory.NewInternalIterator"/>
/// (DBImpl::NewInternalIterator / Version::AddIterators, db/db_impl/db_impl.cc, db/version_set.cc). Unlike the factory,
/// a graph keeps its iterator objects when it is released: rebuilding it for another super version recycles them (table
/// iterators are parked and re-targeted, see <see cref="TableInternalIterator.Retarget"/>), which is how
/// <see cref="Iterator.Refresh()"/> (ArenaWrappedDBIter::Refresh, db/arena_wrapped_db_iter.cc) and the per-column-family
/// pool behind <c>NewIterator</c> (<see cref="IteratorCorePool"/>) avoid allocating an iterator stack per scan.
/// </summary>
/// <remarks>
/// A released graph references no memtable, table reader, block, version or super version (pooled graphs must not keep
/// obsolete files or memtable memory alive). Not thread-safe: owned by one <see cref="IteratorCore"/>.
/// </remarks>
internal sealed class IteratorGraph : IDisposable
{
    private readonly DbImpl _db;
    private readonly ColumnFamilyData _cfd;
    private DbIter? _dbIter;
    private readonly List<MemTableInternalIterator> _mems = [];
    private int _memCount;
    private readonly List<InternalIterator?> _l0 = [];
    private int _l0Count;
    private readonly List<LevelIterator> _levels = [];
    private int _levelCount;
    private readonly List<InternalIterator> _children = [];
    private MergingIterator? _merging;
    private ulong _readahead; // ReadOptions::readahead_size the table iterators were configured with
    private bool _readaheadAsyncIo; // and ReadOptions::async_io (one or two readahead buffers)

    // The view (null when released).
    private SuperVersion? _sv;
    private Snapshot? _ownedSnapshot;
    private ReadCallback? _callback;
    private ReadOptions? _options;
    private bool _hasTombstones;

    public IteratorGraph(DbImpl db, ColumnFamilyData cfd)
    {
        _db = db;
        _cfd = cfd;
    }

    public ColumnFamilyData ColumnFamily => _cfd;

    /// <summary>The DB iterator (null before the first build).</summary>
    public DbIter? DbIter => _dbIter;

    /// <summary>The column family installed another super version since the graph was built.</summary>
    public bool IsStale => _sv is not null && !ReferenceEquals(_cfd.SuperOrNull(), _sv);

    /// <summary>The graph reads through a read callback (WritePrepared / WriteUnprepared): it cannot be refreshed.</summary>
    public bool HasReadCallback => _callback is not null;

    /// <summary>
    /// References the current super version and builds the stack at <paramref name="sequence"/>
    /// (<see cref="DbFormat.MaxSequenceNumber"/> = <paramref name="snapshot"/>'s or the latest). REQUIRES: released.
    /// <paramref name="ownedSnapshot"/> is released with the graph (also when this throws).
    /// </summary>
    public DbIter Build(ReadOptions? options, Snapshot? snapshot, ReadCallback? callback, ulong sequence, Snapshot? ownedSnapshot)
    {
        SuperVersion sv;
        try
        {
            sv = _db.ReferenceIteratorView(_cfd, ref options, snapshot, callback, ref sequence);
        }
        catch
        {
            ownedSnapshot?.Dispose();
            throw;
        }
        SetView(sv, options, callback, ownedSnapshot);
        return BuildStackOrRelease(sequence);
    }

    /// <summary>
    /// Async <see cref="Build"/>: the table files are opened with async I/O first (DbIteratorFactory.CreateDbIterAsync
    /// warms the table cache the same way). Completes synchronously, without an async state machine, when every table
    /// is already open.
    /// </summary>
    public ValueTask<DbIter> BuildAsync(ReadOptions? options, Snapshot? snapshot, CancellationToken cancellationToken)
    {
        ulong sequence = DbFormat.MaxSequenceNumber;
        SuperVersion sv = _db.ReferenceIteratorView(_cfd, ref options, snapshot, null, ref sequence);
        SetView(sv, options, null, null);
        try
        {
            if (options?.ReadTier != ReadTier.MemtableTier)
            {
                VersionStorageInfo s = sv.Current.Storage;
                for (int l = 0; l < s.NumLevels; l++)
                {
                    FileMetaData[] files = s.LevelFilesArray(l);
                    for (int i = 0; i < files.Length; i++)
                    {
                        ValueTask<TableReaderLease> open = _cfd.TableCache.GetReaderAsync(files[i], cancellationToken);
                        if (!open.IsCompletedSuccessfully) return WarmAsync(open, s, l, i, sequence, cancellationToken);
                        open.Result.Dispose();
                    }
                }
            }
        }
        catch
        {
            Release();
            throw;
        }
        return new ValueTask<DbIter>(BuildStackOrRelease(sequence));
    }

    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<DbIter> WarmAsync(ValueTask<TableReaderLease> pending, VersionStorageInfo s, int level, int index, ulong sequence,
        CancellationToken cancellationToken)
    {
        try
        {
            (await pending.ConfigureAwait(false)).Dispose();
            index++;
            for (int l = level; l < s.NumLevels; l++, index = 0)
            {
                FileMetaData[] files = s.LevelFilesArray(l);
                for (; index < files.Length; index++)
                {
                    using TableReaderLease lease = await _cfd.TableCache.GetReaderAsync(files[index], cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            Release();
            throw;
        }
        return BuildStackOrRelease(sequence);
    }

    private void SetView(SuperVersion sv, ReadOptions? options, ReadCallback? callback, Snapshot? ownedSnapshot)
    {
        _sv = sv;
        _options = options;
        _callback = callback;
        _ownedSnapshot = ownedSnapshot;
    }

    private DbIter BuildStackOrRelease(ulong sequence)
    {
        try
        {
            return BuildStack(sequence);
        }
        catch
        {
            Release();
            throw;
        }
    }

    /// <summary>
    /// ArenaWrappedDBIter::Refresh when the super version did not change: only the read sequence (and the range
    /// tombstones visible at it) change. False when the column family installed another super version (before or after
    /// the sequence was read): the caller then releases and rebuilds the graph.
    /// </summary>
    public bool TryRefreshInPlace(Snapshot? snapshot)
    {
        SuperVersion? sv = _sv;
        if (sv is null || _dbIter is null || !ReferenceEquals(_cfd.SuperOrNull(), sv)) return false;
        ulong sequence = _db.IteratorReadSequence(snapshot);
        // A memtable switch between the check above and the sequence read would put writes visible at the sequence in a
        // memtable this super version does not have: check again (upstream re-reads the super version number).
        if (!ReferenceEquals(_cfd.SuperOrNull(), sv)) return false;
        RangeDelAggregator? rangeDel = CollectTombstones(sv, sequence);
        _dbIter.SetSequence(sequence, rangeDel);
        return true;
    }

    /// <summary>
    /// Rebuilds the stack on the current view with other read options (same super version and sequence): used when new
    /// iterate bounds must reach the table iterators (prefix seek captures the upper bound).
    /// </summary>
    public void RebuildWithOptions(ReadOptions? options)
    {
        if (_sv is null || _dbIter is null) throw new InvalidOperationException("the iterator graph is not built");
        ulong sequence = _dbIter.Sequence;
        ParkChildren();
        _options = options;
        BuildStackOrRelease(sequence);
    }

    /// <summary>Sets the bounds level iterators use to skip files.</summary>
    public void SetLevelBounds(ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
    {
        for (int i = 0; i < _levelCount; i++) _levels[i].SetBounds(lower, upper);
    }

    /// <summary>The range tombstones visible at <paramref name="sequence"/> (memtables and files), or null if none.</summary>
    private RangeDelAggregator? CollectTombstones(SuperVersion sv, ulong sequence)
    {
        ReadOptions? options = _options;
        if (options?.IgnoreRangeDeletions == true) return null;
        bool any = _hasTombstones || sv.Mem.NumRangeDeletes > 0;
        if (!any) return null;
        RangeDelAggregator? rangeDel = null;
        AddMemTombstones(sv.Mem, sequence, ref rangeDel);
        foreach (MemTable m in sv.Imm) AddMemTombstones(m, sequence, ref rangeDel);
        if (options?.ReadTier != ReadTier.MemtableTier)
        {
            VersionStorageInfo s = sv.Current.Storage;
            for (int l = 0; l < s.NumLevels; l++)
            {
                foreach (FileMetaData f in s.LevelFilesArray(l)) AddFileTombstones(f, sequence, ref rangeDel);
            }
        }
        return rangeDel;
    }

    private RangeDelAggregator NewAggregator(ulong sequence) =>
        new(_cfd.Icmp, sequence, _options?.Timestamp is { } ts ? ts.Span : default);

    private void AddMemTombstones(MemTable mem, ulong sequence, ref RangeDelAggregator? rangeDel)
    {
        if (mem.NumRangeDeletes == 0) return;
        FragmentedRangeTombstoneList list = mem.GetFragmentedRangeTombstones();
        if (list.IsEmpty) return;
        (rangeDel ??= NewAggregator(sequence)).AddTombstones(list);
    }

    private void AddFileTombstones(FileMetaData file, ulong sequence, ref RangeDelAggregator? rangeDel)
    {
        // The graph holds the super version (and so the version with this file) referenced: the version-scoped lookup
        // takes no table cache reference when the reader is pinned, and the fragmented list is cached in the handle.
        using TableReaderLease lease = _cfd.TableCache.GetReaderForVersion(file, out FragmentedRangeTombstoneList? list);
        if (list is null || list.IsEmpty) return;
        (rangeDel ??= NewAggregator(sequence)).AddTombstones(list, file.Smallest, file.Largest);
    }

    /// <summary>Port of DBImpl::NewInternalIterator + DBIter construction on the current view, recycling iterator objects.</summary>
    private DbIter BuildStack(ulong sequence)
    {
        SuperVersion sv = _sv!;
        ReadOptions? options = _options;
        SliceTransform? pe = sv.MutableOptions.PrefixExtractor;
        ITableCache tc = _cfd.TableCache;
        InternalKeyComparator icmp = _cfd.Icmp;
        ulong readahead = options?.ReadaheadSize ?? 0;
        bool asyncIo = readahead > 0 && options!.AsyncIo;
        if (readahead != _readahead || asyncIo != _readaheadAsyncIo)
        {
            // Table iterators keep a fixed readahead once configured: do not recycle them across that setting.
            for (int i = 0; i < _l0.Count; i++)
            {
                _l0[i]?.Dispose();
                _l0[i] = null;
            }
            foreach (LevelIterator li in _levels) li.DiscardSpare();
            _readahead = readahead;
            _readaheadAsyncIo = asyncIo;
        }
        bool ignoreRangeDel = options?.IgnoreRangeDeletions == true;
        RangeDelAggregator? rangeDel = null;
        _hasTombstones = false;
        _children.Clear();

        AddMemTable(sv.Mem, options, pe, ignoreRangeDel, sequence, ref rangeDel);
        foreach (MemTable m in sv.Imm) AddMemTable(m, options, pe, ignoreRangeDel, sequence, ref rangeDel);

        // NewInternalIterator: with kMemtableTier only the memtables are iterated (no table file, no file tombstone).
        VersionStorageInfo s = sv.Current.Storage;
        if (options?.ReadTier != ReadTier.MemtableTier)
        {
            int numNonEmpty = s.NumNonEmptyLevels;
            // Version::IsFilterSkipped(level) with is_file_last_in_level = false (version_set.cc AddIteratorsForLevel):
            // only the last non-empty level > 0 skips its filters; L0 table iterators always pass skip_filters = false,
            // and so does a tailing ForwardIterator for every level (forward_iterator.cc). The last-L0-file case is
            // for Get / MultiGet only.
            bool optimizeForHits = _cfd.Options.OptimizeFiltersForHits && options?.Tailing != true;
            for (int level = 0; level < s.NumLevels; level++)
            {
                FileMetaData[] files = s.LevelFilesArray(level);
                if (files.Length == 0) continue;
                if (!ignoreRangeDel)
                {
                    foreach (FileMetaData f in files) AddFileTombstones(f, sequence, ref rangeDel);
                }
                if (level == 0)
                {
                    // Merge all level zero files together since they may overlap.
                    bool shouldSample = FileReadSample.ShouldSampleFileRead();
                    for (int fi = 0; fi < files.Length; fi++)
                    {
                        FileMetaData f = files[fi];
                        TableReadFlags readFlags = TableReadFlagsExtensions.FromReadOptions(options);
                        int slot = _l0Count;
                        InternalIterator? reuse = slot < _l0.Count ? _l0[slot] : null;
                        if (slot < _l0.Count) _l0[slot] = null;
                        InternalIterator child = TableInternalIterator.OpenOrRecycle(tc, f, reuse, readFlags, options, pe);
                        if (slot < _l0.Count) _l0[slot] = child;
                        else _l0.Add(child);
                        _l0Count++;
                        // read_options_.iterate_upper_bound of the table iterator (a recycled one was cleared).
                        child.SetIterateUpperBound(options?.IterateUpperBound);
                        if (child is TableInternalIterator t)
                        {
                            if (readahead > 0) t.SetUserReadahead(readahead, asyncIo);
                            // Lets DbIter skip ranges covered by newer range tombstones (SkipCoveredForward / Backward).
                            t.SetSeqnoRange(f.SmallestSeqno, f.LargestSeqno);
                        }
                        _children.Add(child);
                        if (shouldSample) FileReadSample.SampleFileReadInc(f);
                    }
                }
                else
                {
                    LevelIterator li;
                    if (_levelCount < _levels.Count)
                    {
                        li = _levels[_levelCount];
                        li.Reset(files, options, pe);
                    }
                    else
                    {
                        li = new LevelIterator(tc, icmp, files, options, pe);
                        _levels.Add(li);
                    }
                    _levelCount++;
                    if (optimizeForHits && level == numNonEmpty - 1) li.AddReadFlags(TableReadFlags.SkipFilters);
                    _children.Add(li);
                }
            }
        }
        // Immutable memtables and files cannot gain tombstones: TryRefreshInPlace re-collects only when some were seen
        // or the mutable memtable has some.
        _hasTombstones = rangeDel is not null;

        InternalIterator top;
        int n = _children.Count;
        if (n == 0) top = EmptyInternalIterator.Instance;
        else if (n == 1) top = _children[0];
        else
        {
            ReadOnlySpan<InternalIterator> span = System.Runtime.InteropServices.CollectionsMarshal.AsSpan(_children);
            if (_merging is null) _merging = new MergingIterator(icmp, span);
            else _merging.Reset(span);
            top = _merging;
        }

        DbIter it;
        if (_dbIter is null)
        {
            it = _dbIter = new DbIter(icmp, top, sequence, options, _cfd.MergeOperator, rangeDel,
                sv.MutableOptions.MaxSequentialSkipInIterations, cleanup: null, pe);
        }
        else
        {
            it = _dbIter;
            it.Reinitialize(top, sequence, options, _cfd.MergeOperator, rangeDel, sv.MutableOptions.MaxSequentialSkipInIterations, pe);
        }
        if (_callback is not null) it.SetReadCallback(_callback);
        if (_cfd.BlobSource is { } blobs) it.SetBlobSource(blobs, s);
        return it;
    }

    private void AddMemTable(MemTable mem, ReadOptions? options, SliceTransform? pe, bool ignoreRangeDel, ulong sequence,
        ref RangeDelAggregator? rangeDel)
    {
        MemTableInternalIterator m;
        if (_memCount < _mems.Count)
        {
            m = _mems[_memCount];
            m.Reset(mem, options, pe);
        }
        else
        {
            m = new MemTableInternalIterator(mem, options, pe);
            _mems.Add(m);
        }
        _memCount++;
        _children.Add(m);
        if (!ignoreRangeDel) AddMemTombstones(mem, sequence, ref rangeDel);
    }

    /// <summary>
    /// Parks every iterator of the stack (releasing blocks and table reader leases, dropping memtable references) and
    /// detaches the DB iterator, keeping the super version reference.
    /// </summary>
    private void ParkChildren()
    {
        for (int i = 0; i < _memCount; i++) _mems[i].Clear();
        _memCount = 0;
        for (int i = 0; i < _l0Count; i++)
        {
            InternalIterator? c = _l0[i];
            if (c is TableInternalIterator t)
            {
                t.Park();
            }
            else
            {
                c?.Dispose();
                _l0[i] = null;
            }
        }
        _l0Count = 0;
        for (int i = 0; i < _levelCount; i++) _levels[i].Park();
        _levelCount = 0;
        _children.Clear();
        // After the children: blocks they hand over to the pinned iterators manager while parking are released too.
        _dbIter?.Detach();
    }

    /// <summary>
    /// Releases the view: parks the stack, then releases the super version and the owned snapshot
    /// (CleanupSuperVersionHandle / WritePreparedTxnDB::IteratorState). The objects stay for the next build.
    /// </summary>
    public void Release()
    {
        ParkChildren();
        SuperVersion? sv = _sv;
        Snapshot? own = _ownedSnapshot;
        _sv = null;
        _ownedSnapshot = null;
        _callback = null;
        _options = null;
        _hasTombstones = false;
        if (sv is not null) _db.ReturnSuperVersion(sv);
        own?.Dispose();
    }

    public void Dispose()
    {
        Release();
        foreach (MemTableInternalIterator m in _mems) m.Dispose();
        foreach (InternalIterator? c in _l0) c?.Dispose();
        foreach (LevelIterator li in _levels) li.Dispose();
        _merging?.Dispose();
        _dbIter?.Dispose();
        _mems.Clear();
        _l0.Clear();
        _levels.Clear();
    }
}
