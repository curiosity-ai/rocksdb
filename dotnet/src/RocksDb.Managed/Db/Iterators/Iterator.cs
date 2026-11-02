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
/// (the next <c>Next</c>/<c>Seek</c>/<c>MoveNext</c>/<c>MoveNextAsync</c>) or is disposed. Copy what must outlive
/// that. None of the stepping paths allocate.</para>
/// <para>Errors (corruption, I/O, Incomplete when <see cref="ReadOptions.MaxSkippableInternalKeys"/> is exceeded) are
/// thrown as <see cref="RocksDbException"/> by the positioning call. Not thread-safe. Dispose to release the
/// resources (table blocks, super version) it pins.</para>
/// </remarks>
public sealed class Iterator : IDisposable
{
    private readonly DbIter _it;
    private readonly InternalKeyComparator _icmp;
    private bool _disposed;

    /// <summary>Whether the iterator was disposed (WriteUnprepared transactions track their active iterators).</summary>
    internal bool IsDisposed => _disposed;

    /// <summary>Wraps a <see cref="DbIter"/> (ownership transferred). Normally created by <see cref="DbIteratorFactory"/>.</summary>
    public Iterator(DbIter iter)
    {
        ArgumentNullException.ThrowIfNull(iter);
        _it = iter;
        _icmp = iter.Comparator;
        _keyComparator = _icmp.UserComparator.TimestampSize == 0 ? null : _icmp.UserComparator.RootComparator;
    }

    /// <summary>Keys without timestamps are ordered by the root comparator when the column family has timestamps.</summary>
    private readonly Comparator? _keyComparator;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CompareKeys(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => _keyComparator is null ? _icmp.CompareUserKey(a, b) : _keyComparator.Compare(a, b);

    /// <summary>True if positioned at an entry.</summary>
    public bool Valid
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _it.Valid;
    }

    /// <summary>Current user key. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Key => _it.Key;

    /// <summary>Current value. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.</summary>
    public ReadOnlySpan<byte> Value
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            ReadOnlySpan<byte> v = _it.Value;
            int strip = _valueSuffixLength;
            if (strip == 0) return v;
            if (v.Length < strip) ThrowHelper.Corruption("Error: value's length less than timestamp's\n");
            return v[..^strip];
        }
    }

    /// <summary>
    /// Bytes removed from the end of every value (TtlIterator::value() of utilities/ttl/db_ttl_impl.h strips the 4-byte
    /// write time of TTL databases); 0 = none. Set once by the creator before the iterator is handed out.
    /// </summary>
    private int _valueSuffixLength;

    internal void StripValueSuffix(int length) => _valueSuffixLength = length;

    /// <summary>
    /// User-defined timestamp of the current entry (Iterator::timestamp()) for column families with timestamps; empty
    /// otherwise. REQUIRES: <see cref="Valid"/>. Valid until the iterator moves.
    /// </summary>
    public ReadOnlySpan<byte> Timestamp => _it.Timestamp;

    /// <summary>
    /// With <see cref="ReadOptions.IterStartTs"/>: the internal key of the current version (user key | timestamp |
    /// fixed64(seq &lt;&lt; 8 | type)), which tells deletions apart (upstream returns it as key()). Empty otherwise.
    /// </summary>
    public ReadOnlySpan<byte> InternalKey => _it.InternalKey;

    /// <summary>True if the current entry is a wide-column entity (PutEntity). REQUIRES: <see cref="Valid"/>.</summary>
    public bool IsEntity => _it.IsEntity;

    private WideColumns? _columns;
    private Util.PooledBufferWriter? _columnsScratch;

    /// <summary>
    /// The wide columns of the current entry (Iterator::columns): the entity's columns, or a single default column
    /// holding <see cref="Value"/> for a plain value. REQUIRES: <see cref="Valid"/>. The returned object is reused and
    /// refilled on every access; it stays readable after the iterator moves until the next access.
    /// </summary>
    public WideColumns Columns
    {
        get
        {
            WideColumns c = _columns ??= new WideColumns();
            if (_it.IsEntity)
            {
                c.SetSerialized(_it.SerializedEntity);
            }
            else
            {
                Util.PooledBufferWriter w = _columnsScratch ??= new Util.PooledBufferWriter(64);
                w.Clear();
                Db.Wide.WideColumnSerialization.SerializeDefaultColumn(Value, w);
                c.SetSerialized(w.WrittenSpan);
            }
            return c;
        }
    }

    /// <summary>Sequence number of the snapshot the iterator reads.</summary>
    public ulong Sequence => _it.Sequence;

    /// <summary>The user comparator ordering the keys.</summary>
    public Comparator Comparator => _icmp.UserComparator;

    public void SeekToFirst()
    {
        if (_stats is null)
        {
            _it.SeekToFirst();
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.SeekToFirst();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    public void SeekToLast()
    {
        if (_stats is null)
        {
            _it.SeekToLast();
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.SeekToLast();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    /// <summary>Positions at the first key &gt;= <paramref name="target"/>.</summary>
    public void Seek(ReadOnlySpan<byte> target)
    {
        if (_stats is null)
        {
            _it.Seek(target);
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.Seek(target);
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    /// <summary>Positions at the last key &lt;= <paramref name="target"/>.</summary>
    public void SeekForPrev(ReadOnlySpan<byte> target)
    {
        if (_stats is null)
        {
            _it.SeekForPrev(target);
            return;
        }
        long t0 = _stats.StartTimer();
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.SeekForPrev(target);
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordSeek(t0);
    }

    public void Next()
    {
        if (_stats is null)
        {
            _it.Next();
            return;
        }
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.Next();
        }
        finally
        {
            Monitoring.FileReadActivity.Exit(activity);
        }
        RecordStep(Ticker.NumberDbNext, Ticker.NumberDbNextFound);
    }

    public void Prev()
    {
        if (_stats is null)
        {
            _it.Prev();
            return;
        }
        int activity = Monitoring.FileReadActivity.Enter(Histogram.FileReadDbIteratorMicros);
        try
        {
            _it.Prev();
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
        if (_it.Valid)
        {
            st.RecordTick(found);
            st.RecordTick(Ticker.IterBytesRead, (ulong)(_it.Key.Length + _it.Value.Length));
        }
    }

    public ValueTask SeekToFirstAsync(CancellationToken cancellationToken = default) => _it.SeekToFirstAsync(cancellationToken);
    public ValueTask SeekToLastAsync(CancellationToken cancellationToken = default) => _it.SeekToLastAsync(cancellationToken);
    public ValueTask SeekAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) => _it.SeekAsync(target, cancellationToken);
    public ValueTask SeekForPrevAsync(ReadOnlyMemory<byte> target, CancellationToken cancellationToken = default) => _it.SeekForPrevAsync(target, cancellationToken);
    public ValueTask NextAsync(CancellationToken cancellationToken = default) => _it.NextAsync(cancellationToken);
    public ValueTask PrevAsync(CancellationToken cancellationToken = default) => _it.PrevAsync(cancellationToken);

    /// <summary>
    /// Reads the current value with async I/O if it is still pending (a value stored in a blob file is otherwise read
    /// synchronously on first access to <c>Value</c>). Iterator::PrepareValue.
    /// </summary>
    public ValueTask PrepareValueAsync(CancellationToken cancellationToken = default) => _it.PrepareValueAsync(cancellationToken);

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

    /// <summary>Async scan of all entries. Single-use enumerable; see <see cref="KeyValue"/> for the lifetime rule.</summary>
    public IAsyncEnumerable<KeyValue> ScanAsync() => new AsyncScan(this, null, null, ownsIterator: false);

    /// <summary>
    /// Async scan of entries with <paramref name="start"/> &lt;= key &lt; <paramref name="end"/> (null = unbounded).
    /// Single-use. Each <see cref="KeyValue"/> is backed by one buffer reused for every item (no per-item allocation):
    /// it is valid until the next <c>MoveNextAsync</c>.
    /// </summary>
    public IAsyncEnumerable<KeyValue> ScanAsync(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end) =>
        new AsyncScan(this, start, end, ownsIterator: false);

    /// <summary>Like <see cref="ScanAsync(ReadOnlyMemory{byte}?, ReadOnlyMemory{byte}?)"/>, disposing this iterator when the enumeration is disposed.</summary>
    public IAsyncEnumerable<KeyValue> ScanAsyncAndDispose(ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end) =>
        new AsyncScan(this, start, end, ownsIterator: true);

    private sealed class AsyncScan : IAsyncEnumerable<KeyValue>, IAsyncEnumerator<KeyValue>
    {
        private readonly Iterator _iterator;
        private readonly ReadOnlyMemory<byte>? _start;
        private readonly ReadOnlyMemory<byte>? _end;
        private readonly bool _ownsIterator;
        private byte[] _buffer = [];
        private CancellationToken _ct;
        private bool _handedOut;
        private bool _started;
        private bool _done;
        private KeyValue _current;

        public AsyncScan(Iterator iterator, ReadOnlyMemory<byte>? start, ReadOnlyMemory<byte>? end, bool ownsIterator)
        {
            _iterator = iterator;
            _start = start;
            _end = end;
            _ownsIterator = ownsIterator;
        }

        public IAsyncEnumerator<KeyValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (_handedOut) throw new InvalidOperationException("ScanAsync results can be enumerated only once.");
            _handedOut = true;
            _ct = cancellationToken;
            return this;
        }

        public KeyValue Current => _current;

        public ValueTask<bool> MoveNextAsync()
        {
            if (_done) return new ValueTask<bool>(false);
            _ct.ThrowIfCancellationRequested();
            ValueTask t;
            if (!_started)
            {
                _started = true;
                t = _start is ReadOnlyMemory<byte> s ? _iterator._it.SeekAsync(s, _ct) : _iterator._it.SeekToFirstAsync(_ct);
            }
            else
            {
                t = _iterator._it.NextAsync(_ct);
            }
            if (!t.IsCompletedSuccessfully) return MoveNextSlowAsync(t);
            t.GetAwaiter().GetResult();
            // A value in a blob file is read with async I/O before it is captured.
            if (_iterator._it.Valid)
            {
                t = _iterator._it.PrepareValueAsync(_ct);
                if (!t.IsCompletedSuccessfully) return MoveNextSlowAsync(t);
                t.GetAwaiter().GetResult();
            }
            return new ValueTask<bool>(Capture());
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<bool> MoveNextSlowAsync(ValueTask pending)
        {
            await pending.ConfigureAwait(false);
            if (_iterator._it.Valid) await _iterator._it.PrepareValueAsync(_ct).ConfigureAwait(false);
            return Capture();
        }

        private bool Capture()
        {
            DbIter it = _iterator._it;
            if (!it.Valid || (_end is ReadOnlyMemory<byte> e && _iterator.CompareKeys(it.Key, e.Span) >= 0))
            {
                _done = true;
                _current = default;
                return false;
            }
            ReadOnlySpan<byte> k = it.Key, v = _iterator.Value;
            int need = k.Length + v.Length;
            if (_buffer.Length < need)
            {
                byte[] old = _buffer;
                _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(need, old.Length * 2));
                if (old.Length > 0) ArrayPool<byte>.Shared.Return(old);
            }
            k.CopyTo(_buffer);
            v.CopyTo(_buffer.AsSpan(k.Length));
            _current = new KeyValue(_buffer.AsMemory(0, k.Length), _buffer.AsMemory(k.Length, v.Length));
            return true;
        }

        public ValueTask DisposeAsync()
        {
            _current = default;
            _done = true;
            byte[] b = _buffer;
            _buffer = [];
            if (b.Length > 0) ArrayPool<byte>.Shared.Return(b);
            if (_ownsIterator) _iterator.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stats?.RecordTick(Ticker.NoIteratorDeleted);
        _columnsScratch?.Dispose();
        _it.Dispose();
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
