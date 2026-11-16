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
        _pool = pool ?? RocksArrayPool<byte>.Shared;
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
        byte[] newBuffer = _pool.Rent(NextCapacity(_buffer.Length, _written, sizeHint));
        _buffer.AsSpan(0, _written).CopyTo(newBuffer);
        byte[] old = _buffer;
        _buffer = newBuffer;
        if (old.Length > 0) _pool.Return(old);
    }

    /// <summary>
    /// The capacity to grow to: at least what is needed, doubling the current one so that appends stay amortised O(1),
    /// capped at <see cref="Array.MaxLength"/>. Computed in 64 bits: past 1 GiB the doubled length overflows an int,
    /// and taking just what is needed from then on made every small append copy the whole buffer (a 1.5 GiB WAL record
    /// reassembled from 32 KiB fragments never finished).
    /// </summary>
    internal static int NextCapacity(int currentLength, int written, int sizeHint)
    {
        long needed = (long)written + Math.Max(sizeHint, 1);
        if (needed > Array.MaxLength) ThrowTooLarge(needed);
        long doubled = Math.Max((long)currentLength * 2, 256);
        return (int)Math.Min(Math.Max(needed, doubled), Array.MaxLength);
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowTooLarge(long needed) =>
        throw new RocksDbException(StatusCode.NotSupported,
            "Not implemented: a buffer of " + needed + " bytes exceeds the largest .NET array (" + Array.MaxLength + " bytes)");

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
