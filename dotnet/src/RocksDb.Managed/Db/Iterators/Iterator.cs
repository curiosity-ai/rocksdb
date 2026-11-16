using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Iterators;

namespace RocksDb.Managed;

/// <summary>
/// Iterator over the user-visible key/value pairs of a column family at a snapshot. Port of the rocksdb::Iterator
/// surface (include/rocksdb/iterator.h) backed by <see cref="DbIter"/>.
/// </summary>
/// <remarks>
/// <para>Three ways to iterate:</para>
/// <list type="number">
/// <item>Cursor style: <c>for (it.Seek(start); it.Valid; it.Next()) Use(it.Key, it.Value);</c></item>
/// <item><c>foreach</c> over a range with a <c>ref struct</c> enumerator: <c>foreach (KeyValueRef kv in it.Range(start, end))</c>.</item>
/// <item>Async scan: <c>await foreach (KeyValue kv in it.ScanAsync(start, end))</c>.</item>
/// </list>
/// <para><b>Lifetime rule.</b> <see cref="Key"/>, <see cref="Value"/>, <see cref="KeyValueRef"/> and the memory of a
/// <see cref="KeyValue"/> produced by <see cref="ScanAsync()"/> are valid only until the iterator moves again
/// (the next <c>Next</c>/<c>Seek</c>/<c>MoveNext</c>/<c>MoveNextAsync</c>), is refreshed (<see cref="Refresh()"/>,
/// <see cref="SetBounds"/>) or is disposed. Copy what must outlive that. None of the stepping paths allocate.</para>
/// <para><b>Reuse.</b> An iterator created by a DB pins the memtables and files of the moment it was created.
/// <see cref="Refresh()"/> moves it to the current state (or another snapshot) without building a new iterator, and
/// <see cref="SetBounds"/> changes its iterate bounds, so one iterator can serve many short scans. Disposed DB
/// iterators also return their internal iterator stack to a small per-column-family pool, so creating one iterator
/// per scan does not allocate an iterator stack in steady state either. Using an iterator after
/// <see cref="Dispose"/> throws <see cref="ObjectDisposedException"/>.</para>
/// <para>Errors (corruption, I/O, Incomplete when <see cref="ReadOptions.MaxSkippableInternalKeys"/> is exceeded) are
/// thrown as <see cref="RocksDbException"/> by the positioning call. Not thread-safe. Dispose to release the
/// resources (table blocks, super version) it pins.</para>
/// </remarks>
public sealed class Iterator : IDisposable
{
    // The handle is never pooled (its core is): after Dispose, _core is null and every member throws.
    private IteratorCore? _core;
    // The core's DB iterator (the same object for the core's whole life) and value suffix, for the accessors called per
    // entry (Valid / Key / Value) without going through the core. Cleared on Dispose like _core.
    private DbIter? _it;
    private int _valueSuffixLength;
    private AsyncScan? _scan;
    private WideColumns? _columns;

    /// <summary>Wraps a <see cref="DbIter"/> (ownership transferred). DB iterators are created by <c>NewIterator</c>.</summary>
    public Iterator(DbIter iter)
    {
        ArgumentNullException.ThrowIfNull(iter);
        _core = new IteratorCore(iter);
        _it = iter;
    }

    internal Iterator(IteratorCore core)
    {
        _core = core;
        _it = core.It;
    }

    private DbIter It
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            DbIter? it = _it;
            if (it is null) ThrowDisposed();
            return it;
        }
    }

    private IteratorCore Core
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            IteratorCore? c = _core;
            if (c is null) ThrowDisposed();
            return c;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowDisposed() => throw new ObjectDisposedException(nameof(Iterator));

    /// <summary>Whether the iterator was disposed (WriteUnprepared transactions track their active iterators).</summary>
    internal bool IsDisposed => _core is null;

    /// <summary>The wrapped DB iterator (tests).</summary>
    internal DbIter Inner => Core.It;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKeys(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Core.CompareKeys(a, b);

    /// <summary>True if positioned at an entry.</summary>
    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => It.Valid;
    }

    /// <summary>Current user key. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Key
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => It.Key;
    }

    /// <summary>Current value. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ReadOnlySpan<byte> v = It.Value;
            int strip = _valueSuffixLength;
            if (strip == 0) return v;
            if (v.Length < strip) ThrowHelper.Corruption("Error: value's length less than timestamp's\n");
            return v[..^strip];
        }
    }

    internal void StripValueSuffix(int length)
    {
        Core.ValueSuffixLength = length;
        _valueSuffixLength = length;
    }

    /// <summary>
    /// User-defined timestamp of the current entry (Iterator::timestamp()) for column families with timestamps; empty
    /// otherwise. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.
    /// </summary>
    public ReadOnlySpan<byte> Timestamp => Core.It.Timestamp;

    /// <summary>
    /// With <see cref="ReadOptions.IterStartTs"/>: the internal key of the current version (user key | timestamp |
    /// fixed64(seq &lt;&lt; 8 | type)), which tells deletions apart (upstream returns it as key()). Empty otherwise.
    /// </summary>
    public ReadOnlySpan<byte> InternalKey => Core.It.InternalKey;

    /// <summary>True if the current entry is a wide-column entity (PutEntity). REQUIRES: <see cref="Valid"/>.</summary>
    public bool IsEntity => Core.It.IsEntity;

    /// <summary>
    /// The wide columns of the current entry (Iterator::columns): the entity's columns, or a single default column
    /// holding <see cref="Value"/> for a plain value. REQUIRES: <see cref="Valid"/>. The returned object belongs to this
    /// iterator and is refilled on every access; it stays readable after the iterator moves until the next access.
    /// </summary>
    public WideColumns Columns
    {
        get
        {
            IteratorCore core = Core;
            WideColumns c = _columns ??= new WideColumns();
            core.FillColumns(c);
            return c;
        }
    }

    /// <summary>Sequence number of the snapshot the iterator reads.</summary>
    public ulong Sequence => Core.It.Sequence;

    /// <summary>The user comparator ordering the keys.</summary>
    public Comparator Comparator => Core.Comparator.UserComparator;

    /// <summary>Positions at the first entry (at <see cref="ReadOptions.IterateLowerBound"/> when set).</summary>
    public void SeekToFirst() => Core.SeekToFirst();

    /// <summary>Positions at the last entry (before <see cref="ReadOptions.IterateUpperBound"/> when set).</summary>
    public void SeekToLast() => Core.SeekToLast();

    /// <summary>Positions at the first key &gt;= <paramref name="target"/>.</summary>
    public void Seek(ReadOnlySpan<byte> target) => Core.Seek(target);

    /// <summary>Positions at the last key &lt;= <paramref name="target"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target) => Core.SeekForPrev(target);

    /// <summary>Moves to the next entry. REQUIRES: <see cref="Valid"/>.</summary>
    public void Next() => Core.Next();

    /// <summary>Moves to the previous entry. REQUIRES: <see cref="Valid"/>.</summary>
    public void Prev() => Core.Prev();

    /// <summary>Statistics to record NUMBER_DB_SEEK / NEXT / PREV (+ FOUND), ITER_BYTES_READ and DB_SEEK into.</summary>
    internal Statistics? Statistics
    {
        get => Core.Statistics;
        set => Core.Statistics = value;
    }

    /// <summary>Records seeks in the DB's query trace while one runs (set by the DB when it creates the iterator).</summary>
    internal void SetTraceState(DbImpl db, uint cfId, ReadOptions? options) => Core.SetTraceState(db, cfId, options);

    /// <summary>Async <see cref="SeekToFirst"/>: data blocks are read with async I/O; completes synchronously on cache hits.</summary>
    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) => Track(Core.SeekToFirstAsync(cancellationToken));

    /// <summary>Async <see cref="SeekToLast"/>.</summary>
    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default) => Track(Core.SeekToLastAsync(cancellationToken));

    /// <summary>Async <see cref="Seek"/>.</summary>
    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) => Track(Core.SeekAsync(target, cancellationToken));

    /// <summary>Async <see cref="SeekForPrev"/>.</summary>
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) =>
        Track(Core.SeekForPrevAsync(target, cancellationToken));

    /// <summary>Async <see cref="Next"/>.</summary>
    public ValueTask NextAsync(CancellationToken cancellationToken = default) => Track(Core.NextAsync(cancellationToken));

    /// <summary>Async <see cref="Prev"/>.</summary>
    public ValueTask PrevAsync(CancellationToken cancellationToken = default) => Track(Core.PrevAsync(cancellationToken));

    /// <summary>
    /// Reads the current value with async I/O if it is still pending (a value stored in a blob file is otherwise read
    /// synchronously on first access to <c>Value</c>). Iterator::PrepareValue.
    /// </summary>
    public ValueTask PrepareValueAsync(CancellationToken cancellationToken = default) => Track(Core.It.PrepareValueAsync(cancellationToken));

    // ------------------------------------------------------------------ refresh and bounds

    /// <summary>
    /// Re-targets the iterator at the latest state of the DB (Iterator::Refresh, include/rocksdb/iterator_base.h): later
    /// positioning calls see the writes, flushes and compactions made since it was created or last refreshed, also when
    /// it was created with a <see cref="ReadOptions.Snapshot"/>. The iterator is unpositioned afterwards (seek before
    /// reading) and the internal iterator stack is reused, so this is much cheaper than creating a new iterator and does
    /// not allocate in steady state. Spans obtained before the call are invalid afterwards.
    /// </summary>
    /// <exception cref="RocksDbException"><see cref="StatusCode.NotSupported"/> for iterators that cannot be refreshed:
    /// tailing iterators (they follow new writes on their own), iterators of WritePrepared / WriteUnprepared transaction
    /// DBs and transactions, and iterators not created by a DB (e.g. <see cref="SstFileReader"/>).</exception>
    /// <remarks>If the stack has to be rebuilt and that fails (the database was disposed, a table file cannot be opened,
    /// the call was canceled), the iterator is disposed and the exception is rethrown: it no longer holds a view to read.
    /// A snapshot of another DB, or a released one, throws InvalidArgument and leaves the iterator unchanged.</remarks>
    public void Refresh() => Refresh(null);

    /// <summary>
    /// Like <see cref="Refresh()"/>, reading at <paramref name="snapshot"/> (null = the latest state) from now on
    /// (Iterator::Refresh(const Snapshot*)). The snapshot must belong to the iterator's DB and stay alive while the
    /// iterator reads at it.
    /// </summary>
    public void Refresh(Snapshot? snapshot)
    {
        IteratorCore core = Core;
        try
        {
            core.Refresh(snapshot);
        }
        catch when (core.IsUnbuilt)
        {
            // The view was released and the rebuild failed: positioning calls would otherwise run on a parked stack and
            // report an empty iterator.
            Dispose();
            throw;
        }
    }

    /// <summary>Async <see cref="Refresh()"/>: when the DB's files changed, the new table files are opened with async I/O.</summary>
    public ValueTask RefreshAsync(CancellationToken cancellationToken = default) => RefreshAsync(null, cancellationToken);

    /// <summary>Async <see cref="Refresh(Snapshot?)"/>.</summary>
    public ValueTask RefreshAsync(Snapshot? snapshot, CancellationToken cancellationToken = default)
    {
        IteratorCore core = Core;
        ValueTask t;
        try
        {
            t = core.RefreshAsync(snapshot, cancellationToken);
        }
        catch when (core.IsUnbuilt)
        {
            Dispose();
            throw;
        }
        // A rebuild that waits for a file read is a pending async move: tracked, so that a Dispose meanwhile (including the
        // one below on a failed rebuild) defers the release of the core until the move is done.
        return t.IsCompletedSuccessfully ? t : Track(AwaitRebuildAsync(t, core));
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitRebuildAsync(ValueTask pending, IteratorCore core)
    {
        try
        {
            await pending.ConfigureAwait(false);
        }
        catch when (core.IsUnbuilt && ReferenceEquals(_core, core))
        {
            Dispose();
            throw;
        }
    }

    // ------------------------------------------------------------------ async moves in flight
    //
    // An async move that has to wait for a file read keeps running on the core after it returned. If the iterator is
    // disposed meanwhile, the core must not go back to the column family's pool before that move is done: the next
    // NewIterator would rent it, and the move would then reposition (or invalidate) that other iterator. Dispose
    // therefore hands the core over to the move, which releases it when it completes.

    private const int Idle = 0, MovePending = 1, DisposeRequested = 2;
    private int _moveState;
    private IteratorCore? _deferredCore;

    /// <summary>Returns <paramref name="move"/>; one that has not completed is tracked until it does (no allocation either way).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask Track(ValueTask move) => move.IsCompleted ? move : TrackSlowAsync(move);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask TrackSlowAsync(ValueTask move)
    {
        BeginPendingMove();
        try
        {
            await move.ConfigureAwait(false);
        }
        finally
        {
            EndPendingMove();
        }
    }

    /// <summary>An async move (or scan step) is about to wait: Dispose defers the release of the core until <see cref="EndPendingMove"/>.</summary>
    private void BeginPendingMove() => Volatile.Write(ref _moveState, MovePending);

    /// <summary>The pending async move is done; releases the core if the iterator was disposed meanwhile.</summary>
    private void EndPendingMove()
    {
        if (Interlocked.Exchange(ref _moveState, Idle) != DisposeRequested) return;
        IteratorCore core = _deferredCore!;
        _deferredCore = null;
        ReleaseCore(core);
    }

    /// <summary>
    /// Replaces the iterate bounds the iterator was created with (<see cref="ReadOptions.IterateLowerBound"/> inclusive,
    /// <see cref="ReadOptions.IterateUpperBound"/> exclusive; null = unbounded), so one iterator can scan many ranges.
    /// The iterator is unpositioned afterwards; the bounds take effect at the next positioning call and stay in effect
    /// across <see cref="Refresh()"/>. The caller's <see cref="ReadOptions"/> object is not modified. The bound memory
    /// must stay unchanged while the iterator uses it. Allocation-free, except on column families with a prefix
    /// extractor, where it copies the read options once per call.
    /// </summary>
    public void SetBounds(ReadOnlyMemory<byte>? lowerBound, ReadOnlyMemory<byte>? upperBound)
    {
        IteratorCore core = Core;
        try
        {
            core.SetBounds(lowerBound, upperBound);
        }
        catch when (core.IsUnbuilt)
        {
            // A prefix seek iterator rebuilds its stack here (see Refresh).
            Dispose();
            throw;
        }
    }

    // ------------------------------------------------------------------ foreach over a range (ref struct)

    /// <summary><c>foreach</c> over all entries (in key order) from the first.</summary>
    public RangeEnumerator GetEnumerator() => new(this, default, false, default, false);

    /// <summary>All entries, in key order.</summary>
    public RangeEnumerable Range() => new(this, default, false, default, false);

    /// <summary>Entries with key &gt;= <paramref name="start"/>.</summary>
    public RangeEnumerable Range(ReadOnlySpan<byte> start) => new(this, start, true, default, false);

    /// <summary>Entries with <paramref name="start"/> &lt;= key &lt; <paramref name="end"/>.</summary>
    public RangeEnumerable Range(ReadOnlySpan<byte> start, ReadOnlySpan<byte> end) => new(this, start, true, end, true);

    /// <summary>A key range to <c>foreach</c> over; enumerating repositions the iterator.</summary>
    public readonly ref struct RangeEnumerable
    {
        private readonly Iterator _iterator;
        private readonly ReadOnlySpan<byte> _start;
        private readonly ReadOnlySpan<byte> _end;
        private readonly bool _hasStart;
        private readonly bool _hasEnd;

        internal RangeEnumerable(Iterator iterator, ReadOnlySpan<byte> start, bool hasStart, ReadOnlySpan<byte> end, bool hasEnd)
        {
            _iterator = iterator;
            _start = start;
            _hasStart = hasStart;
            _end = end;
            _hasEnd = hasEnd;
        }

        public RangeEnumerator GetEnumerator() => new(_iterator, _start, _hasStart, _end, _hasEnd);
    }

    /// <summary>Allocation-free enumerator; <see cref="Current"/> is valid until the next <see cref="MoveNext"/>.</summary>
    public ref struct RangeEnumerator
    {
        private readonly Iterator _iterator;
        private readonly ReadOnlySpan<byte> _start;
        private readonly ReadOnlySpan<byte> _end;
        private readonly bool _hasStart;
        private readonly bool _hasEnd;
        private bool _started;

        internal RangeEnumerator(Iterator iterator, ReadOnlySpan<byte> start, bool hasStart, ReadOnlySpan<byte> end, bool hasEnd)
        {
            _iterator = iterator;
            _start = start;
            _hasStart = hasStart;
            _end = end;
            _hasEnd = hasEnd;
            _started = false;
        }

        public bool MoveNext()
        {
            Iterator it = _iterator;
            if (!_started)
            {
                _started = true;
                if (_hasStart) it.Seek(_start);
                else it.SeekToFirst();
            }
            else
            {
                it.Next();
            }
            return it.Valid && (!_hasEnd || it.CompareKeys(it.Key, _end) < 0);
        }

        public readonly KeyValueRef Current => new(_iterator.Key, _iterator.Value);
    }

    // ------------------------------------------------------------------ async scan

    /// <summary>
    /// Async scan of all entries. See <see cref="ScanAsync(ReadOnlyMemory{byte}?, ReadOnlyMemory{byte}?)"/> for the reuse
    /// and lifetime rules.
    /// </summary>
    public IAsyncEnumerable<KeyValue> ScanAsync() => StartScan(null, null, ownsIterator: false);

    /// <summary>
    /// Async scan of entries with <paramref name="start"/> &lt;= key &lt; <paramref name="end"/> (null = unbounded).
    /// Each <see cref="KeyValue"/> is backed by one buffer reused for every item (no per-item allocation): it is valid
    /// until the next <c>MoveNextAsync</c>. The returned enumerable can be enumerated once; the iterator reuses one scan
    /// object for all its scans (no allocation per scan), so a new <c>ScanAsync</c> call is only allowed after the
    /// previous enumeration was disposed (<c>await foreach</c> does that), else it throws
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    public IAsyncEnumerable<KeyValue> ScanAsync(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end) =>
        StartScan(start, end, ownsIterator: false);

    /// <summary>Like <see cref="ScanAsync(ReadOnlyMemory{byte}?, ReadOnlyMemory{byte}?)"/>, disposing this iterator when the enumeration is disposed.</summary>
    public IAsyncEnumerable<KeyValue> ScanAsyncAndDispose(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end) =>
        StartScan(start, end, ownsIterator: true);

    private AsyncScan StartScan(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end, bool ownsIterator)
    {
        _ = Core; // ObjectDisposedException
        AsyncScan s = _scan ??= new AsyncScan(this);
        s.Reset(start, end, ownsIterator);
        return s;
    }

    /// <summary>The scan of one iterator, reset by every <c>ScanAsync</c> call (one enumeration at a time).</summary>
    private sealed class AsyncScan : IAsyncEnumerable<KeyValue>, IAsyncEnumerator<KeyValue>
    {
        private readonly Iterator _iterator;
        private ReadOnlyMemory<byte>? _start;
        private ReadOnlyMemory<byte>? _end;
        private bool _ownsIterator;
        private byte[] _buffer = [];
        private CancellationToken _ct;
        private bool _handedOut;
        private bool _active;
        private bool _started;
        private bool _done;
        private KeyValue _current;

        public AsyncScan(Iterator iterator) => _iterator = iterator;

        public void Reset(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end, bool ownsIterator)
        {
            if (_active || Volatile.Read(ref _iterator._moveState) != Idle)
                throw new InvalidOperationException("The previous ScanAsync enumeration of this iterator was not disposed.");
            _start = start;
            _end = end;
            _ownsIterator = ownsIterator;
            _ct = default;
            _handedOut = false;
            _started = false;
            _done = false;
            _current = default;
        }

        public IAsyncEnumerator<KeyValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (_handedOut) throw new InvalidOperationException("ScanAsync results can be enumerated only once.");
            _handedOut = true;
            _active = true;
            _ct = cancellationToken;
            return this;
        }

        public KeyValue Current => _current;

        public ValueTask<bool> MoveNextAsync()
        {
            // A disposed enumeration returns false (also when ScanAsyncAndDispose disposed the iterator with it); one whose
            // iterator was disposed under it throws.
            if (!_active) return new ValueTask<bool>(false);
            if (_iterator._core is null) ThrowDisposed();
            if (_done) return new ValueTask<bool>(false);
            // IAsyncEnumerator: one MoveNextAsync at a time. A second call while a step waits for a read would move the
            // iterator under it.
            if (Volatile.Read(ref _iterator._moveState) != Idle) ThrowConcurrentMoveNext();
            _ct.ThrowIfCancellationRequested();
            // The moves go through the core like Seek / Next of the sync range enumerator: statistics (NUMBER_DB_SEEK /
            // NEXT, ITER_BYTES_READ), query tracing and the renewal of a tailing iterator apply to scans too.
            IteratorCore core = _iterator.Core;
            ValueTask t;
            if (!_started)
            {
                _started = true;
                t = _start is ReadOnlyMemory<byte> s ? core.SeekAsync(s, _ct) : core.SeekToFirstAsync(_ct);
            }
            else
            {
                t = core.NextAsync(_ct);
            }
            if (!t.IsCompletedSuccessfully) return MoveNextSlowAsync(t);
            t.GetAwaiter().GetResult();
            DbIter it = core.It;
            // A value in a blob file is read with async I/O before it is captured.
            if (it.Valid)
            {
                t = it.PrepareValueAsync(_ct);
                if (!t.IsCompletedSuccessfully) return MoveNextSlowAsync(t);
                t.GetAwaiter().GetResult();
            }
            return new ValueTask<bool>(Capture());
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<bool> MoveNextSlowAsync(ValueTask pending)
        {
            // A disposal of the iterator meanwhile keeps the core and the item buffer until this step is done (see Track).
            _iterator.BeginPendingMove();
            try
            {
                await pending.ConfigureAwait(false);
                DbIter it = _iterator.Core.It;
                if (it.Valid) await it.PrepareValueAsync(_ct).ConfigureAwait(false);
                return Capture();
            }
            finally
            {
                _iterator.EndPendingMove();
            }
        }

        private bool Capture()
        {
            IteratorCore core = _iterator.Core;
            DbIter it = core.It;
            if (!it.Valid || (_end is ReadOnlyMemory<byte> e && core.CompareKeys(it.Key, e.Span) >= 0))
            {
                _done = true;
                _current = default;
                return false;
            }
            ReadOnlySpan<byte> k = it.Key, v = core.Value;
            int need = k.Length + v.Length;
            if (_buffer.Length < need)
            {
                byte[] old = _buffer;
                _buffer = RocksArrayPool<byte>.Shared.Rent(Math.Max(need, old.Length * 2));
                if (old.Length > 0) RocksArrayPool<byte>.Shared.Return(old);
            }
            k.CopyTo(_buffer);
            v.CopyTo(_buffer.AsSpan(k.Length));
            _current = new KeyValue(_buffer.AsMemory(0, k.Length), _buffer.AsMemory(k.Length, v.Length));
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        [System.Diagnostics.CodeAnalysis.DoesNotReturn]
        private static void ThrowConcurrentMoveNext() =>
            throw new InvalidOperationException("MoveNextAsync was called while the previous MoveNextAsync of this scan (or an async move of its iterator) is still running.");

        public ValueTask DisposeAsync()
        {
            if (!_active) return ValueTask.CompletedTask;
            _active = false;
            _current = default;
            _done = true;
            if (_ownsIterator) _iterator.Dispose();
            return ValueTask.CompletedTask;
        }

        /// <summary>Returns the item buffer to the array pool (the iterator is disposed).</summary>
        public void ReleaseBuffer()
        {
            // _active stays: MoveNextAsync of an enumeration whose iterator was disposed under it throws ObjectDisposedException.
            _done = true;
            _current = default;
            byte[] b = _buffer;
            _buffer = [];
            if (b.Length > 0) RocksArrayPool<byte>.Shared.Return(b);
        }
    }

    /// <summary>Releases what the iterator pins; a DB iterator's internal stack goes back to the column family's pool.</summary>
    public void Dispose()
    {
        IteratorCore? core = Interlocked.Exchange(ref _core, null);
        if (core is null) return;
        _it = null;
        // An async move still waiting for a file read releases the core when it completes (EndPendingMove).
        _deferredCore = core;
        if (Interlocked.CompareExchange(ref _moveState, DisposeRequested, MovePending) == MovePending) return;
        _deferredCore = null;
        ReleaseCore(core);
    }

    private void ReleaseCore(IteratorCore core)
    {
        _scan?.ReleaseBuffer();
        core.Release();
    }
}

/// <summary>A key/value pair as spans, produced by <see cref="Iterator.RangeEnumerator"/>; valid until the iterator moves.</summary>
public readonly ref struct KeyValueRef
{
    public KeyValueRef(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Key = key;
        Value = value;
    }

    public ReadOnlySpan<byte> Key { get; }
    public ReadOnlySpan<byte> Value { get; }

    public void Deconstruct(out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        key = Key;
        value = Value;
    }
}

/// <summary>
/// A key/value pair produced by an async scan. The memory belongs to the scan's reusable buffer and is valid until
/// the next <c>MoveNextAsync</c> (or until the enumerator is disposed); use <see cref="KeyToArray"/> /
/// <see cref="ValueToArray"/> (or copy the memory) to keep it.
/// </summary>
public readonly struct KeyValue
{
    public KeyValue(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        Key = key;
        Value = value;
    }

    public ReadOnlyMemory<byte> Key { get; }
    public ReadOnlyMemory<byte> Value { get; }
    public ReadOnlySpan<byte> KeySpan => Key.Span;
    public ReadOnlySpan<byte> ValueSpan => Value.Span;

    public byte[] KeyToArray() => Key.ToArray();
    public byte[] ValueToArray() => Value.ToArray();

    public void Deconstruct(out ReadOnlyMemory<byte> key, out ReadOnlyMemory<byte> value)
    {
        key = Key;
        value = Value;
    }
}
