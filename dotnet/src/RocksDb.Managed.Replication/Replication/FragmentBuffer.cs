namespace RocksDb.Managed.Replication;

/// <summary>
/// The receiving side of a payload sent in pieces (a large write batch on a WAL stream, a large client write): one
/// buffer of the announced total length, rented from <see cref="RocksArrayPool{T}.Shared"/>, filled strictly in order.
/// The receiver holds the whole payload in memory - the pieces only keep every single message small.
/// Not thread-safe.
/// </summary>
public sealed class FragmentBuffer : IDisposable
{
    private byte[]? _buffer;
    private int _total;
    private int _received;

    /// <summary>Whether a buffer is held (a payload is being received, or a complete one was not released yet).</summary>
    public bool IsActive => _buffer is not null;

    /// <summary>The announced length of the payload being received (0 when none).</summary>
    public int TotalLength => _total;

    /// <summary>How many bytes of it arrived - the offset the next piece must have.</summary>
    public int ReceivedLength => _received;

    public bool IsComplete => _buffer is not null && _received == _total;

    /// <summary>The bytes received so far; valid until <see cref="Release"/>, <see cref="Begin"/> or disposal.</summary>
    public ReadOnlyMemory<byte> Memory => _buffer is null ? default : _buffer.AsMemory(0, _received);

    /// <summary>Starts a payload of <paramref name="totalLength"/> bytes (releasing whatever was held). The caller checks its own size limit first.</summary>
    public void Begin(int totalLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(totalLength);
        Release();
        _buffer = RocksArrayPool<byte>.Shared.Rent(totalLength);
        _total = totalLength;
    }

    /// <summary>
    /// Adds the piece at <paramref name="offset"/>. False (nothing is copied) unless a payload is being received, the
    /// piece is not empty, starts exactly where the previous one ended and ends within the announced length.
    /// </summary>
    public bool TryAppend(int offset, ReadOnlySpan<byte> data)
    {
        if (_buffer is null || data.IsEmpty || offset != _received || data.Length > _total - _received) return false;
        data.CopyTo(_buffer.AsSpan(_received));
        _received += data.Length;
        return true;
    }

    /// <summary>Returns the buffer to the pool. Idempotent.</summary>
    public void Release()
    {
        byte[]? b = _buffer;
        _buffer = null;
        _total = 0;
        _received = 0;
        if (b is not null) RocksArrayPool<byte>.Shared.Return(b);
    }

    public void Dispose() => Release();
}
