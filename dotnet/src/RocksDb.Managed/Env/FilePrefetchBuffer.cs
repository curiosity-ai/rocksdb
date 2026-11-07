using System.Buffers;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Env;

/// <summary>
/// Lite port of FilePrefetchBuffer (file/file_prefetch_buffer.{h,cc}): a single pooled buffer in front of an
/// <see cref="IRandomAccessFile"/> for sequential-ish access (iterators, compaction inputs, footer/metadata reads).
/// <para>
/// Auto-readahead semantics follow BlockBasedTableIterator's: readahead starts at
/// <c>initialReadaheadSize</c> and doubles on each sequential miss up to <c>maxReadaheadSize</c>; a non-sequential
/// miss resets it. A read that hits the buffer is served by copy (or zero-copy via <see cref="TryGetSpan"/>).
/// </para>
/// Not thread-safe; use one instance per iterator/compaction input. Dispose to return the buffer to the pool.
/// </summary>
public sealed class FilePrefetchBuffer : IDisposable
{
    private readonly IRandomAccessFile _file;
    private readonly int _initialReadahead;
    private readonly int _maxReadahead;
    private byte[] _buffer = [];
    private long _bufferOffset;      // file offset of _buffer[0]
    private int _bufferLength;       // valid bytes
    private int _readahead;
    private long _prevEnd = -1;      // end offset of the previous request (sequentiality detection)

    public FilePrefetchBuffer(IRandomAccessFile file, int initialReadaheadSize, int maxReadaheadSize)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentOutOfRangeException.ThrowIfNegative(initialReadaheadSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxReadaheadSize, initialReadaheadSize);
        _file = file;
        _initialReadahead = initialReadaheadSize;
        _maxReadahead = maxReadaheadSize;
        _readahead = initialReadaheadSize;
    }

    /// <summary>Current readahead size (exposed for tests/diagnostics).</summary>
    public int ReadaheadSize => _readahead;

    /// <summary>Number of physical reads issued (tests/diagnostics).</summary>
    public int PhysicalReads { get; private set; }

    /// <summary>
    /// Returns a view of [offset, offset+length) if fully buffered (TryReadFromCache hit). The span is valid until the
    /// next call on this instance.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetSpan(long offset, int length, out ReadOnlySpan<byte> data)
    {
        long rel = offset - _bufferOffset;
        if (rel >= 0 && rel + length <= _bufferLength)
        {
            data = _buffer.AsSpan((int)rel, length);
            _prevEnd = offset + length;
            return true;
        }
        data = default;
        return false;
    }

    /// <summary>Reads [offset, offset+destination.Length), prefetching on a miss. Returns bytes read (short at EOF).</summary>
    public int Read(long offset, Span<byte> destination)
    {
        if (TryGetSpan(offset, destination.Length, out ReadOnlySpan<byte> hit))
        {
            hit.CopyTo(destination);
            return destination.Length;
        }
        int want = PrepareMiss(offset, destination.Length);
        _bufferLength = _file.Read(offset, _buffer.AsSpan(0, want));
        _bufferOffset = offset;
        PhysicalReads++;
        return CopyOut(offset, destination);
    }

    /// <summary>Async variant of <see cref="Read"/>; completes synchronously on a buffer hit.</summary>
    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (TryGetSpan(offset, destination.Length, out ReadOnlySpan<byte> hit))
        {
            hit.CopyTo(destination.Span);
            return new ValueTask<int>(destination.Length);
        }
        return ReadMissAsync(offset, destination, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadMissAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        int want = PrepareMiss(offset, destination.Length);
        _bufferLength = await _file.ReadAsync(offset, _buffer.AsMemory(0, want), cancellationToken).ConfigureAwait(false);
        _bufferOffset = offset;
        PhysicalReads++;
        return CopyOut(offset, destination.Span);
    }

    private int PrepareMiss(long offset, int length)
    {
        bool sequential = offset == _prevEnd || (offset >= _bufferOffset && offset <= _bufferOffset + _bufferLength && _bufferLength > 0);
        if (sequential)
        {
            if (PhysicalReads > 0) _readahead = Math.Min(_maxReadahead, Math.Max(_readahead * 2, _initialReadahead));
        }
        else
        {
            _readahead = _initialReadahead;
        }
        long remaining = Math.Max(0, _file.Length - offset);
        int want = (int)Math.Min(Math.Max((long)length, Math.Min((long)length + _readahead, remaining)), int.MaxValue);
        if (_buffer.Length < want)
        {
            byte[] old = _buffer;
            _buffer = ArrayPool<byte>.Shared.Rent(want);
            if (old.Length > 0) ArrayPool<byte>.Shared.Return(old);
        }
        _bufferLength = 0;
        return want;
    }

    private int CopyOut(long offset, Span<byte> destination)
    {
        int n = Math.Min(destination.Length, _bufferLength);
        _buffer.AsSpan(0, n).CopyTo(destination);
        _prevEnd = offset + n;
        return n;
    }

    public void Dispose()
    {
        byte[] old = _buffer;
        _buffer = [];
        _bufferLength = 0;
        if (old.Length > 0) ArrayPool<byte>.Shared.Return(old);
    }
}
