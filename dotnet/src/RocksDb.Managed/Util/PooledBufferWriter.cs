using System.Buffers;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Growable <see cref="IBufferWriter{T}"/> backed by <see cref="ArrayPool{T}"/>. The workhorse buffer of
/// the port: block builders, WAL record assembly, WriteBatch reps, etc. Call <see cref="Dispose"/> to return
/// the array to the pool. Not thread-safe. Being a class lets it be reused (Clear) across operations and
/// pooled itself (see <see cref="ObjectPools"/>).
/// </summary>
public sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;
    private readonly ArrayPool<byte> _pool;

    public PooledBufferWriter(int initialCapacity = 256, ArrayPool<byte>? pool = null)
    {
        _pool = pool ?? ArrayPool<byte>.Shared;
        _buffer = initialCapacity == 0 ? [] : _pool.Rent(initialCapacity);
    }

    public int WrittenCount => _written;
    public int Capacity => _buffer.Length;
    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);
    /// <summary>Mutable view over written bytes (e.g. to patch a header in place).</summary>
    public Span<byte> WrittenSpanMutable => _buffer.AsSpan(0, _written);

    /// <summary>Resets the written count but keeps the rented buffer.</summary>
    public void Clear() => _written = 0;

    /// <summary>Truncates the written region to <paramref name="length"/> bytes.</summary>
    public void Truncate(int length)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)length, (uint)_written);
        _written = length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Advance(int count)
    {
        if ((uint)count > (uint)(_buffer.Length - _written)) ThrowAdvance();
        _written += count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        if (_buffer.Length - _written < Math.Max(sizeHint, 1)) Grow(sizeHint);
        return _buffer.AsSpan(_written);
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_buffer.Length - _written < Math.Max(sizeHint, 1)) Grow(sizeHint);
        return _buffer.AsMemory(_written);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(ReadOnlySpan<byte> data)
    {
        if (_buffer.Length - _written < data.Length) Grow(data.Length);
        data.CopyTo(_buffer.AsSpan(_written));
        _written += data.Length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteByte(byte b)
    {
        if (_written == _buffer.Length) Grow(1);
        _buffer[_written++] = b;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(int sizeHint)
    {
        int needed = _written + Math.Max(sizeHint, 1);
        int newSize = Math.Max(needed, Math.Max(_buffer.Length * 2, 256));
        if ((uint)newSize > (uint)Array.MaxLength) newSize = Math.Max(needed, Array.MaxLength);
        byte[] newBuffer = _pool.Rent(newSize);
        _buffer.AsSpan(0, _written).CopyTo(newBuffer);
        byte[] old = _buffer;
        _buffer = newBuffer;
        if (old.Length > 0) _pool.Return(old);
    }

    private static void ThrowAdvance() => throw new InvalidOperationException("Cannot advance past the end of the buffer.");

    public void Dispose()
    {
        byte[] old = _buffer;
        _buffer = [];
        _written = 0;
        if (old.Length > 0) _pool.Return(old);
    }
}

/// <summary>Shared object pools for frequently created helper objects.</summary>
internal static class ObjectPools
{
    [ThreadStatic] private static PooledBufferWriter? t_cachedWriter;

    /// <summary>Gets a cleared thread-local writer if available, else a new one. Return with <see cref="ReturnWriter"/>.</summary>
    public static PooledBufferWriter RentWriter()
    {
        PooledBufferWriter? w = t_cachedWriter;
        if (w is not null)
        {
            t_cachedWriter = null;
            w.Clear();
            return w;
        }
        return new PooledBufferWriter(1024);
    }

    public static void ReturnWriter(PooledBufferWriter w)
    {
        if (w.Capacity > 1 << 20) { w.Dispose(); return; } // don't cache huge buffers
        t_cachedWriter = w;
    }
}
