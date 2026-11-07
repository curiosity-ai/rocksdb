using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Env;

/// <summary>
/// Native memory block aligned for direct I/O (port of util/aligned_buffer.h's AlignedBuffer storage). Exposed as a
/// <see cref="MemoryManager{T}"/> so it can be handed to <see cref="RandomAccess"/> async APIs without pinning.
/// Owned by a single user at a time; return to <see cref="AlignedBufferPool"/> or dispose (there is no finalizer,
/// see CA2015, so an undisposed buffer leaks its native memory).
/// </summary>
internal sealed unsafe class AlignedBuffer : MemoryManager<byte>
{
    private void* _ptr;

    public AlignedBuffer(int size, int alignment)
    {
        if (!BitOperations_IsPow2(alignment)) ThrowHelper.InvalidArgument("alignment must be a power of two");
        Capacity = RoundUp(size, alignment);
        Alignment = alignment;
        _ptr = NativeMemory.AlignedAlloc((nuint)Capacity, (nuint)alignment);
        GC.AddMemoryPressure(Capacity);
    }

    public int Capacity { get; }
    public int Alignment { get; }

    public Span<byte> Span => new(_ptr, Capacity);

    public override Span<byte> GetSpan() => new(_ptr, Capacity);

    public override MemoryHandle Pin(int elementIndex = 0) => new((byte*)_ptr + elementIndex);

    public override void Unpin() { }

    protected override void Dispose(bool disposing)
    {
        if (_ptr != null)
        {
            NativeMemory.AlignedFree(_ptr);
            _ptr = null;
            GC.RemoveMemoryPressure(Capacity);
        }
    }


    public static int RoundUp(int value, int alignment) => (value + alignment - 1) & ~(alignment - 1);
    public static long RoundDown(long value, int alignment) => value & ~(long)(alignment - 1);
    private static bool BitOperations_IsPow2(int v) => v > 0 && (v & (v - 1)) == 0;
}

/// <summary>Small process-wide pool of <see cref="AlignedBuffer"/>s used as bounce buffers for direct reads.</summary>
internal static class AlignedBufferPool
{
    private const int MaxPooled = 32;
    private static readonly ConcurrentQueue<AlignedBuffer> s_pool = new();
    private static int s_count;

    public static AlignedBuffer Rent(int minSize, int alignment)
    {
        while (s_pool.TryDequeue(out AlignedBuffer? b))
        {
            Interlocked.Decrement(ref s_count);
            if (b.Capacity >= minSize && b.Alignment == alignment) return b;
            ((IDisposable)b).Dispose();
        }
        return new AlignedBuffer(Math.Max(minSize, 64 * 1024), alignment);
    }

    public static void Return(AlignedBuffer b)
    {
        if (Interlocked.Increment(ref s_count) <= MaxPooled)
        {
            s_pool.Enqueue(b);
            return;
        }
        Interlocked.Decrement(ref s_count);
        ((IDisposable)b).Dispose();
    }
}
