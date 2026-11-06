using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Iterators;

/// <summary>
/// The state behind a public <see cref="Iterator"/>: the <see cref="DbIter"/>, statistics / tracing / deadline / tailing
/// hooks, and for DB iterators the re-targetable <see cref="IteratorGraph"/> that <see cref="Refresh"/> and the
/// per-column-family <see cref="IteratorCorePool"/> recycle. Port of the ArenaWrappedDBIter part of
/// db/arena_wrapped_db_iter.cc (Refresh, the DBIter + internal iterator stack it owns).
/// </summary>
/// <remarks>
/// A core is pooled, its handle is not: every <c>NewIterator</c> returns a new <see cref="Iterator"/> object that drops
/// its core on dispose, so a disposed iterator throws <see cref="ObjectDisposedException"/> instead of reading the
/// stack another iterator now uses. Not thread-safe.
/// </remarks>
internal sealed class IteratorCore
{
    /// <summary>The DB iterator (replaced only for cores over a caller-built DbIter: never for graph cores).</summary>
    internal DbIter It;
    private readonly InternalKeyComparator _icmp;
    /// <summary>Keys without timestamps are ordered by the root comparator when the column family has timestamps.</summary>
    private readonly Comparator? _keyComparator;
    private readonly IteratorGraph? _graph;
    private readonly IteratorCorePool? _pool;

    // Rebuild state of graph cores.
    private ReadOptions? _options;        // the caller's read options
    private ReadOptions? _boundsOptions;  // a copy of _options with SetBounds' bounds (prefix seek; see ApplyBounds)
    private bool _tailing;
    private bool _allowRefresh;
    private bool _boundsSet;
    private ReadOnlyMemory<byte>? _lower;
    private ReadOnlyMemory<byte>? _upper;
    private PooledBufferWriter? _tailKey;

    /// <summary>A core over a DB iterator built by the caller (ownership transferred; not pooled, not refreshable).</summary>
    public IteratorCore(DbIter iter)
    {
        It = iter;
        _icmp = iter.Comparator;
        _keyComparator = _icmp.UserComparator.TimestampSize == 0 ? null : _icmp.UserComparator.RootComparator;
    }

    /// <summary>A graph core of <paramref name="pool"/> (unbuilt until <see cref="Build"/>).</summary>
    public IteratorCore(DbImpl db, ColumnFamilyData cfd, IteratorCorePool pool)
    {
        _graph = new IteratorGraph(db, cfd);
        _pool = pool;
        It = null!;
        _icmp = cfd.Icmp;
        _keyComparator = _icmp.UserComparator.TimestampSize == 0 ? null : _icmp.UserComparator.RootComparator;
    }

    public InternalKeyComparator Comparator => _icmp;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareKeys(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => _keyComparator is null ? _icmp.CompareUserKey(a, b) : _keyComparator.Compare(a, b);

    // ------------------------------------------------------------------ building, refresh, bounds

    /// <summary>Builds the graph for a new iterator (see <see cref="IteratorGraph.Build"/>).</summary>
    public void Build(ReadOptions? options, Snapshot? snapshot, ReadCallback? callback, ulong sequence, Snapshot? ownedSnapshot, bool tailing)
    {
        IteratorGraph g = _graph!;
        _options = options;
        _tailing = tailing;
        // ArenaWrappedDBIter::allow_refresh_: false for WritePrepared / WriteUnprepared reads (read callback).
        _allowRefresh = callback is null && !tailing;
        It = g.Build(options, snapshot, callback, sequence, ownedSnapshot);
    }

    /// <summary>Async <see cref="Build"/> of a plain iterator (no read callback, not tailing).</summary>
    public ValueTask BuildAsync(ReadOptions? options, CancellationToken cancellationToken)
    {
        _options = options;
        _tailing = false;
        _allowRefresh = true;
        ValueTask<DbIter> t = _graph!.BuildAsync(options, options?.Snapshot, cancellationToken);
        if (!t.IsCompletedSuccessfully) return AwaitBuildAsync(t);
        It = t.Result;
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitBuildAsync(ValueTask<DbIter> pending) => It = await pending.ConfigureAwait(false);

    [MethodImpl(MethodImplOptions.NoInlining)]
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void RefreshNotAllowed() =>
        throw new RocksDbException(StatusCode.NotSupported, "Not implemented: Creating renew iterator is not allowed.");

    private IteratorGraph RefreshableGraph()
    {
        IteratorGraph? g = _graph;
        if (g is null || !_allowRefresh) RefreshNotAllowed();
        return g;
    }

    /// <summary>
    /// Port of ArenaWrappedDBIter::Refresh(const Snapshot*): re-targets the iterator at the latest DB state (or
    /// <paramref name="snapshot"/>), unpositioned; on an unchanged super version only the read sequence changes,
    /// otherwise the stack is rebuilt (recycling its objects) on the current super version.
    /// </summary>
    public void Refresh(Snapshot? snapshot)
    {
        IteratorGraph g = RefreshableGraph();
        if (!g.TryRefreshInPlace(snapshot))
        {
            g.Release();
            It = g.Build(_boundsOptions ?? _options, snapshot, null, DbFormat.MaxSequenceNumber, null);
        }
        ApplyBoundsAfterBuild();
    }

    /// <summary>Async <see cref="Refresh"/>: table files of a new super version are opened with async I/O.</summary>
    public ValueTask RefreshAsync(Snapshot? snapshot, CancellationToken cancellationToken)
    {
        IteratorGraph g = RefreshableGraph();
        cancellationToken.ThrowIfCancellationRequested();
        if (g.TryRefreshInPlace(snapshot))
        {
            ApplyBoundsAfterBuild();
            return ValueTask.CompletedTask;
        }
        g.Release();
        ValueTask<DbIter> t = g.BuildAsync(_boundsOptions ?? _options, snapshot, cancellationToken);
        if (!t.IsCompletedSuccessfully) return AwaitRefreshAsync(t);
        It = t.Result;
        ApplyBoundsAfterBuild();
        return ValueTask.CompletedTask;
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitRefreshAsync(ValueTask<DbIter> pending)
    {
        It = await pending.ConfigureAwait(false);
        ApplyBoundsAfterBuild();
    }

    /// <summary>Replaces the iterate bounds; the iterator is unpositioned afterwards.</summary>
    public void SetBounds(ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
    {
        _boundsSet = true;
        _lower = lower;
        _upper = upper;
        _boundsOptions = null;
        if (It.PrefixExtractor is not null)
        {
            // Prefix seek: table iterators captured the upper bound (auto_prefix_mode compares it with the seek key), so
            // the stack is rebuilt with options carrying the new bounds (one copy of the read options per call).
            if (_graph is null) TailingNotSupported("SetBounds on a prefix seek iterator that is not a DB iterator");
            ReadOptions o = It.ReadOptions?.Clone() ?? new ReadOptions();
            o.IterateLowerBound = lower;
            o.IterateUpperBound = upper;
            _boundsOptions = o;
            _graph.RebuildWithOptions(o);
            return;
        }
        It.SetBounds(lower, upper);
        _graph?.SetLevelBounds(lower, upper);
    }

    /// <summary>Re-applies SetBounds after the stack was rebuilt from the read options.</summary>
    private void ApplyBoundsAfterBuild()
    {
        if (!_boundsSet || _boundsOptions is not null) return; // the options of the build carried them
        It.SetBounds(_lower, _upper);
        _graph?.SetLevelBounds(_lower, _upper);
    }

    // ------------------------------------------------------------------ value / columns

    /// <summary>
    /// Bytes removed from the end of every value (TtlIterator::value() of utilities/ttl/db_ttl_impl.h strips the 4-byte
    /// write time of TTL databases); 0 = none. Set once by the creator before the iterator is handed out.
    /// </summary>
    internal int ValueSuffixLength;

    public ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ReadOnlySpan<byte> v = It.Value;
            int strip = ValueSuffixLength;
            if (strip == 0) return v;
            if (v.Length < strip) ThrowHelper.Corruption("Error: value's length less than timestamp's\n");
            return v[..^strip];
        }
    }

    private PooledBufferWriter? _columnsScratch;

    /// <summary>Fills <paramref name="c"/> with the wide columns of the current entry (Iterator::columns).</summary>
    public void FillColumns(WideColumns c)
    {
        if (It.IsEntity)
        {
            c.SetSerialized(It.SerializedEntity);
        }
        else
        {
            PooledBufferWriter w = _columnsScratch ??= new PooledBufferWriter(64);
            w.Clear();
            Wide.WideColumnSerialization.SerializeDefaultColumn(Value, w);
            c.SetSerialized(w.WrittenSpan);
        }
    }

    // ------------------------------------------------------------------ positioning

    public void SeekToFirst()
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.SeekToFirst, default);
            return;
        }
        if (_tailing) RenewIfStale();
        if (_stats is null)
        {
            It.SeekToFirst();
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.SeekToFirst();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    public void SeekToLast()
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.SeekToLast, default);
            return;
        }
        if (_tailing) TailingNotSupported("ForwardIterator::SeekToLast()");
        if (_stats is null)
        {
            It.SeekToLast();
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.SeekToLast();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    // Query tracing (DBIter::has_trace_state_ / trace_db_ / trace_cf_id_ / trace_read_options_).
    private DbImpl? _traceDb;
    private uint _traceCfId;
    private ReadOptions? _traceReadOptions;

    /// <summary>Records seeks in the DB's query trace while one runs (set by the DB when it creates the iterator).</summary>
    internal void SetTraceState(DbImpl db, uint cfId, ReadOptions? options)
    {
        _traceDb = db;
        _traceCfId = cfId;
        _traceReadOptions = options;
        if (options is not null && options.Deadline > TimeSpan.Zero) _deadlineOptions = options;
    }

    // ReadOptions::deadline: table file reads of the iterator's moves fail with TimedOut once it has passed.
    private ReadOptions? _deadlineOptions;

    private enum DeadlineMove : byte { SeekToFirst, SeekToLast, Seek, SeekForPrev, Next, Prev }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void MoveWithDeadline(DeadlineMove move, ReadOnlySpan<byte> target)
    {
        ReadOptions o = _deadlineOptions!;
        _deadlineOptions = null;
        long token = Table.ReadDeadline.Enter(o);
        try
        {
            switch (move)
            {
                case DeadlineMove.SeekToFirst: SeekToFirst(); break;
                case DeadlineMove.SeekToLast: SeekToLast(); break;
                case DeadlineMove.Seek: Seek(target); break;
                case DeadlineMove.SeekForPrev: SeekForPrev(target); break;
                case DeadlineMove.Next: Next(); break;
                default: Prev(); break;
            }
        }
        finally
        {
            Table.ReadDeadline.Exit(token);
            _deadlineOptions = o;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void TraceSeek(Tracing.QueryTracer t, Tracing.TraceType type, ReadOnlySpan<byte> target)
    {
        ReadOnlySpan<byte> lower = It.LowerBound is { } lb ? lb.Span : default;
        ReadOnlySpan<byte> upper = It.UpperBound is { } ub ? ub.Span : default;
        t.IteratorSeek(type, _traceCfId, target, lower, upper, _traceReadOptions);
    }

    public void Seek(ReadOnlySpan<byte> target)
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.Seek, target);
            return;
        }
        if (_tailing) RenewIfStale();
        if (_traceDb?.QueryTracer is { } tracer) TraceSeek(tracer, Tracing.TraceType.IteratorSeek, target);
        if (_stats is null)
        {
            It.Seek(target);
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.Seek(target);
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.SeekForPrev, target);
            return;
        }
        if (_tailing) TailingNotSupported("ForwardIterator::SeekForPrev()");
        if (_traceDb?.QueryTracer is { } tracer) TraceSeek(tracer, Tracing.TraceType.IteratorSeekForPrev, target);
        if (_stats is null)
        {
            It.SeekForPrev(target);
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.SeekForPrev(target);
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    public void Next()
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.Next, default);
            return;
        }
        if (_tailing && _graph!.IsStale && TailingRenewForNext()) return;
        if (_stats is null)
        {
            It.Next();
            return;
        }
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.Next();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordStep(Ticker.NumberDbNext, Ticker.NumberDbNextFound);
    }

    public void Prev()
    {
        if (_deadlineOptions is not null)
        {
            MoveWithDeadline(DeadlineMove.Prev, default);
            return;
        }
        if (_tailing) TailingNotSupported("ForwardIterator::Prev");
        if (_stats is null)
        {
            It.Prev();
            return;
        }
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            It.Prev();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordStep(Ticker.NumberDbPrev, Ticker.NumberDbPrevFound);
    }

    // ---- statistics (DBIter::Seek / Next / Prev RecordTick calls) ----

    private Statistics? _stats;

    /// <summary>Statistics to record NUMBER_DB_SEEK / NEXT / PREV (+ FOUND), ITER_BYTES_READ and DB_SEEK into.</summary>
    internal Statistics? Statistics
    {
        get => _stats;
        set
        {
            _stats = value;
            value?.RecordTick(Ticker.NoIteratorCreated);
        }
    }

    private void RecordSeek(long startTimestamp)
    {
        Statistics st = _stats!;
        st.RecordElapsedMicros(Histogram.DbSeek, startTimestamp);
        RecordStep(Ticker.NumberDbSeek, Ticker.NumberDbSeekFound);
    }

    private void RecordStep(Ticker step, Ticker found)
    {
        Statistics st = _stats!;
        st.RecordTick(step);
        if (It.Valid)
        {
            st.RecordTick(found);
            st.RecordTick(Ticker.IterBytesRead, (ulong)(It.Key.Length + It.Value.Length));
        }
    }

    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken)
    {
        if (_tailing) RenewIfStale();
        return It.SeekToFirstAsync(cancellationToken);
    }

    public ValueTask SeekToLastAsync(CancellationToken cancellationToken)
    {
        if (_tailing) TailingNotSupported("ForwardIterator::SeekToLast()");
        return It.SeekToLastAsync(cancellationToken);
    }

    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken)
    {
        if (_tailing) RenewIfStale();
        if (_traceDb?.QueryTracer is { } tracer) TraceSeek(tracer, Tracing.TraceType.IteratorSeek, target.Span);
        return It.SeekAsync(target, cancellationToken);
    }

    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken)
    {
        if (_tailing) TailingNotSupported("ForwardIterator::SeekForPrev()");
        if (_traceDb?.QueryTracer is { } tracer) TraceSeek(tracer, Tracing.TraceType.IteratorSeekForPrev, target.Span);
        return It.SeekForPrevAsync(target, cancellationToken);
    }

    public ValueTask NextAsync(CancellationToken cancellationToken)
    {
        if (_tailing && _graph!.IsStale && TailingRenewForNext()) return ValueTask.CompletedTask;
        return It.NextAsync(cancellationToken);
    }

    public ValueTask PrevAsync(CancellationToken cancellationToken)
    {
        if (_tailing) TailingNotSupported("ForwardIterator::Prev");
        return It.PrevAsync(cancellationToken);
    }

    // ---- tailing (ReadOptions::tailing, ForwardIterator) ----
    //
    // A tailing iterator (port of the ForwardIterator behaviour of DBImpl::NewIterator) reads at kMaxSequenceNumber,
    // so writes made after its creation become visible to later Seek / Next calls, and its memtable / table iterators
    // are rebuilt on the current super version when the column family installed a new one
    // (ForwardIterator::RenewIterators on Seek, SeekToFirst and Next). SeekToLast, SeekForPrev and Prev are NotSupported
    // like upstream. Deviation: the stack is the regular DBIter stack rebuilt on renewal (recycling its objects), not
    // ForwardIterator's incremental per-level iterators.

    private void RenewIfStale()
    {
        IteratorGraph g = _graph!;
        if (!g.IsStale) return;
        g.Release();
        It = g.Build(_boundsOptions ?? _options, null, null, DbFormat.MaxSequenceNumber, null);
        ApplyBoundsAfterBuild();
    }

    /// <summary>
    /// ForwardIterator::Next after a super version change: renew, re-seek to the current key and advance past it.
    /// Returns true when the re-seek already moved past the old key (nothing left to do).
    /// </summary>
    private bool TailingRenewForNext()
    {
        PooledBufferWriter current = _tailKey ??= new PooledBufferWriter(64);
        current.Clear();
        current.Write(It.Key);
        RenewIfStale();
        It.Seek(current.WrittenSpan);
        return !It.Valid || CompareKeys(It.Key, current.WrittenSpan) != 0;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void TailingNotSupported(string op) =>
        throw new RocksDbException(StatusCode.NotSupported, "Not implemented: " + op);

    // ------------------------------------------------------------------ release

    /// <summary>
    /// Ends the use of this core by its handle: a graph core releases its view (super version, blocks, files) and goes
    /// back to its pool; a core over a caller-built DbIter disposes it.
    /// </summary>
    public void Release()
    {
        _stats?.RecordTick(Ticker.NoIteratorDeleted);
        if (_pool is null)
        {
            _columnsScratch?.Dispose();
            It.Dispose();
            return;
        }
        _pool.Return(this);
    }

    /// <summary>Releases the view and clears per-iterator settings before the core is pooled (or dropped).</summary>
    internal void ResetForPool()
    {
        _graph!.Release();
        _options = null;
        _boundsOptions = null;
        _tailing = false;
        _allowRefresh = false;
        _boundsSet = false;
        _lower = _upper = null;
        _stats = null;
        _traceDb = null;
        _traceReadOptions = null;
        _deadlineOptions = null;
        ValueSuffixLength = 0;
    }

    /// <summary>Disposes everything (a core the full pool does not take).</summary>
    internal void DisposeAll()
    {
        _graph?.Dispose();
        _columnsScratch?.Dispose();
        _columnsScratch = null;
        _tailKey?.Dispose();
        _tailKey = null;
    }
}

/// <summary>
/// A small bounded pool of <see cref="IteratorCore"/>s (iterator graphs) of one column family: <c>NewIterator</c>
/// rents one and the iterator's <c>Dispose</c> returns it, released (no super version, block, table reader or memtable
/// is referenced while pooled). Lock-free: a fixed array of slots taken and filled with interlocked exchanges; cores
/// beyond its capacity are disposed.
/// </summary>
internal sealed class IteratorCorePool
{
    /// <summary>Pooled cores per column family: enough for a few concurrent scans per core, small enough to bound memory.</summary>
    internal const int Capacity = 8;

    private readonly IteratorCore?[] _slots = new IteratorCore?[Capacity];

    /// <summary>Number of pooled cores (tests).</summary>
    internal int Count
    {
        get
        {
            int n = 0;
            for (int i = 0; i < _slots.Length; i++) if (Volatile.Read(ref _slots[i]) is not null) n++;
            return n;
        }
    }

    public IteratorCore Rent(DbImpl db, ColumnFamilyData cfd)
    {
        IteratorCore?[] slots = _slots;
        for (int i = 0; i < slots.Length; i++)
        {
            if (Volatile.Read(ref slots[i]) is not null && Interlocked.Exchange(ref slots[i], null) is { } core) return core;
        }
        return new IteratorCore(db, cfd, this);
    }

    public void Return(IteratorCore core)
    {
        try
        {
            core.ResetForPool();
        }
        catch
        {
            // Releasing the view failed (e.g. the super version cleanup threw): do not reuse the objects.
            core.DisposeAll();
            throw;
        }
        IteratorCore?[] slots = _slots;
        for (int i = 0; i < slots.Length; i++)
        {
            if (Volatile.Read(ref slots[i]) is null && Interlocked.CompareExchange(ref slots[i], core, null) is null) return;
        }
        core.DisposeAll();
    }
}
