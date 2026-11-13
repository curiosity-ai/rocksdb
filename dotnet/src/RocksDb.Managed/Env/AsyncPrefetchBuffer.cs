using System.Buffers;

namespace RocksDb.Managed.Env;

/// <summary>
/// Sequential readahead with the next chunk read asynchronously while the current one is consumed: a port of the
/// async_io mode of FilePrefetchBuffer (file/file_prefetch_buffer.{h,cc}: two buffers, PrefetchAsync of the second
/// while the first serves reads, PollIfNeeded / swap on a miss), as BlockPrefetcher uses it for compaction inputs with
/// compaction_readahead_size (table/block_based/block_prefetcher.cc).
/// </summary>
/// <remarks>
/// <para>A miss outside the buffered range reads one readahead chunk synchronously from the requested offset and
/// starts the read of the following chunk with <see cref="IRandomAccessFile.ReadAsync"/>; a sequential reader then
/// finds most blocks in memory and only waits when it outruns the prefetch, so file I/O overlaps the caller's CPU work
/// (decompression, merging, building the output). A file system whose ReadAsync completes synchronously (page-cache
/// hits with RWF_NOWAIT, in-memory files) still gets the larger reads.</para>
/// <para>The prefetch target is chunk-aligned to the previous read's end, so a block may straddle two chunks: the
/// prefetch lands behind a reserved gap at the start of its buffer and the straddling tail of the current chunk is
/// copied into that gap on the swap (no re-read). Blocks larger than the gap are read directly.</para>
/// <para>Not thread-safe (one per iterator). Buffers come from <see cref="RocksArrayPool{T}.Shared"/>; dispose (or
/// <see cref="Reset"/>) waits for a pending prefetch before reusing them.</para>
/// <para>Allocation-free in steady state: the pending read is kept as its <see cref="ValueTask{TResult}"/> (no
/// <c>AsTask</c> / <c>Task.FromResult</c> per prefetch); one that has not completed when it is started gets a cached
/// continuation that records its result and signals a reusable event the reader waits on (a pooled
/// <see cref="System.Threading.Tasks.Sources.IValueTaskSource{TResult}"/> must not be blocked on directly).</para>
/// </remarks>
public sealed class AsyncPrefetchBuffer : IDisposable
{
    private const int MinReserve = 64 * 1024;

    private int _readahead;
    private readonly int _initialReadahead;
    private readonly int _maxReadahead;
    private IRandomAccessFile? _file;
    private long _fileLength;
    private int _reserve = MinReserve;

    // Current window: _cur[_curStart .. _curStart + _curLen) holds the file bytes [_curOffset, _curOffset + _curLen).
    private byte[] _cur = [];
    private int _curStart;
    private int _curLen;
    private long _curOffset;

    // Prefetch: _next[_nextStart ..] receives the file bytes from _nextOffset.
    private byte[] _next = [];
    private int _nextStart;
    private long _nextOffset = -1;

    // The pending prefetch: _hasPending while a read was issued and its result not consumed. A read that completed
    // synchronously stores its outcome directly (_pendingSync); otherwise OnPrefetchCompleted stores it and sets _done,
    // and the reader always waits on _done. Setting the event is the continuation's last access to this object, so once
    // the reader's wait returns the buffer may be disposed or reused. (A completion flag written by the continuation
    // before the event let the reader skip the wait and dispose the buffer while the continuation was still about to
    // set the event: a NullReferenceException on a thread-pool thread, which ends the process.)
    private bool _hasPending;
    private bool _pendingSync;
    private ValueTask<int> _pendingTask;
    private int _pendingResult;
    private Exception? _pendingError;
    private ManualResetEventSlim? _done;
    private Action? _onCompleted;

    /// <param name="readaheadSize">Bytes per read (compaction_readahead_size).</param>
    public AsyncPrefetchBuffer(int readaheadSize) : this(readaheadSize, readaheadSize)
    {
    }

    /// <summary>
    /// A buffer whose reads start at <paramref name="initialReadaheadSize"/> bytes and double with every read up to
    /// <paramref name="maxReadaheadSize"/> (FilePrefetchBuffer with implicit auto readahead: initial_auto_readahead_size,
    /// max_auto_readahead_size).
    /// </summary>
    public AsyncPrefetchBuffer(int initialReadaheadSize, int maxReadaheadSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(initialReadaheadSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxReadaheadSize, initialReadaheadSize);
        _readahead = initialReadaheadSize;
        _initialReadahead = initialReadaheadSize;
        _maxReadahead = maxReadaheadSize;
    }

    /// <summary>The size of the next read.</summary>
    public int ReadaheadSize => _readahead;

    /// <summary>
    /// Drops the buffered data and restarts the readahead at its initial size (a non-sequential access:
    /// FilePrefetchBuffer::ResetValues).
    /// </summary>
    public void ResetReadahead()
    {
        Reset(_file);
        _readahead = _initialReadahead;
    }

    /// <summary>Keeps exponentially increasing the readahead size until the maximum.</summary>
    private void GrowReadahead()
    {
        if (_readahead < _maxReadahead) _readahead = (int)Math.Min(_maxReadahead, (long)_readahead * 2);
    }

    /// <summary>Synchronous file reads issued on misses (tests / diagnostics).</summary>
    public int SyncReads { get; private set; }

    /// <summary>Prefetches issued (tests / diagnostics).</summary>
    public int Prefetches { get; private set; }

    /// <summary>Prefetches the reader had to wait for (tests / diagnostics).</summary>
    public int PrefetchWaits { get; private set; }

    /// <summary>Switches to <paramref name="file"/> (or none), dropping the buffered data.</summary>
    public void Reset(IRandomAccessFile? file)
    {
        WaitPendingIgnoringErrors();
        _file = file;
        _fileLength = file?.Length ?? 0;
        _curLen = 0;
        _curOffset = 0;
        _nextOffset = -1;
    }

    /// <summary>
    /// Returns the file bytes [offset, offset + length) from the buffers, reading (and prefetching) as needed. The span
    /// is valid until the next call. False if the range is beyond the end of the file.
    /// </summary>
    public bool TryRead(long offset, int length, out ReadOnlySpan<byte> data)
    {
        if (TryGetBuffered(offset, length, out data)) return true;
        if (_hasPending && offset >= _curOffset && offset <= _nextOffset && TrySwap(offset, length, out data)) return true;
        return ReadMiss(offset, length, out data);
    }

    private bool TryGetBuffered(long offset, int length, out ReadOnlySpan<byte> data)
    {
        long rel = offset - _curOffset;
        if (rel >= 0 && rel + length <= _curLen)
        {
            data = new ReadOnlySpan<byte>(_cur, _curStart + (int)rel, length);
            return true;
        }
        data = default;
        return false;
    }

    /// <summary>Moves to the prefetched chunk, keeping the tail of the current one from <paramref name="offset"/>.</summary>
    private bool TrySwap(long offset, int length, out ReadOnlySpan<byte> data)
    {
        if (!_pendingSync && !_done!.IsSet) PrefetchWaits++;
        int n = TakePending();
        int tail = (int)(_nextOffset - offset); // bytes of the requested range still in the current window
        if (tail > _nextStart)
        {
            data = default;
            return false;
        }
        if (tail > 0) Array.Copy(_cur, _curStart + (int)(offset - _curOffset), _next, _nextStart - tail, tail);
        (_cur, _next) = (_next, _cur);
        _curStart = _nextStart - tail;
        _curLen = tail + n;
        _curOffset = offset;
        StartPrefetch();
        return TryGetBuffered(offset, length, out data);
    }

    private bool ReadMiss(long offset, int length, out ReadOnlySpan<byte> data)
    {
        WaitPendingIgnoringErrors();
        if (length > _reserve) _reserve = length; // later prefetches leave room for a straddling block this large
        long remaining = _fileLength - offset;
        if (remaining < length)
        {
            data = default;
            return false;
        }
        int want = (int)Math.Min(Math.Max(length, _readahead), remaining);
        if (_cur.Length < want) Rent(ref _cur, want);
        int got = _file!.Read(offset, _cur.AsSpan(0, want));
        SyncReads++;
        GrowReadahead();
        _curStart = 0;
        _curLen = got;
        _curOffset = offset;
        if (got < length)
        {
            data = default;
            return false;
        }
        StartPrefetch();
        data = new ReadOnlySpan<byte>(_cur, 0, length);
        return true;
    }

    private void StartPrefetch()
    {
        long start = _curOffset + _curLen;
        _nextOffset = start;
        if (start >= _fileLength) return;
        int len = (int)Math.Min(_readahead, _fileLength - start);
        _nextStart = _reserve;
        if (_next.Length < _reserve + len) Rent(ref _next, _reserve + len);
        Prefetches++;
        GrowReadahead();
        ValueTask<int> t = _file!.ReadAsync(start, _next.AsMemory(_nextStart, len));
        _hasPending = true;
        _pendingError = null;
        if (t.IsCompleted)
        {
            // Completed synchronously (page-cache hit, in-memory file): consume it now.
            try
            {
                _pendingResult = t.GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                _pendingError = e;
            }
            _pendingSync = true;
            return;
        }
        _pendingSync = false;
        _pendingTask = t;
        ManualResetEventSlim done = _done ??= new ManualResetEventSlim(false);
        done.Reset();
        t.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(_onCompleted ??= OnPrefetchCompleted);
    }

    /// <summary>Continuation of a prefetch that completed asynchronously: records its outcome and wakes the reader.</summary>
    private void OnPrefetchCompleted()
    {
        try
        {
            _pendingResult = _pendingTask.GetAwaiter().GetResult();
        }
        catch (Exception e)
        {
            _pendingError = e;
        }
        _pendingTask = default;
        _done!.Set();
    }

    /// <summary>Waits for the pending prefetch and returns its byte count (rethrows its failure). PRE: one is pending.</summary>
    private int TakePending()
    {
        if (!_pendingSync) _done!.Wait();
        _hasPending = false;
        Exception? e = _pendingError;
        _pendingError = null;
        if (e is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(e);
        return _pendingResult;
    }

    private static void Rent(ref byte[] buffer, int size)
    {
        byte[] old = buffer;
        buffer = RocksArrayPool<byte>.Shared.Rent(size);
        if (old.Length > 0) RocksArrayPool<byte>.Shared.Return(old);
    }

    private void WaitPendingIgnoringErrors()
    {
        if (!_hasPending) return;
        try
        {
            TakePending();
        }
        catch (Exception)
        {
            // A failed prefetch is not an error until its data is needed; the next read re-reads synchronously.
        }
    }

    public void Dispose()
    {
        WaitPendingIgnoringErrors();
        _file = null;
        _curLen = 0;
        if (_cur.Length > 0) RocksArrayPool<byte>.Shared.Return(_cur);
        if (_next.Length > 0) RocksArrayPool<byte>.Shared.Return(_next);
        _cur = [];
        _next = [];
        // Not disposed: it never allocates a wait handle (WaitHandle is not used), and a Set still unwinding on the
        // completing thread must not meet a disposed event.
        _done = null;
    }
}
