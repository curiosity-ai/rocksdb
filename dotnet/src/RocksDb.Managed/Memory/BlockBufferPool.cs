using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Memory;

/// <summary>
/// Pool for block buffers (data/index blocks kept in the block cache) with finer size classes than
/// <see cref="ArrayPool{T}.Shared"/>. The shared pool rounds every request up to a power of two, so a typical 4 KiB
/// data block (4096+ bytes payload plus the 5-byte trailer) occupies an 8 KiB array: half of a block cache's capacity
/// would be padding. Here requests above 4 KiB use four size classes per power of two (at most 25% padding, usually
/// much less: a 4101-byte block gets a 5 KiB array), closer to upstream, which charges malloc's usable size.
/// </summary>
/// <remarks>
/// <para>Power-of-two sizes (including everything up to 4 KiB and above 1 MiB) are delegated to
/// <see cref="ArrayPool{T}.Shared"/>, so <see cref="Return"/> accepts any array obtained from either pool and routes
/// it by length. Arrays whose length is not a class size are dropped (left to the GC).</para>
/// <para>Each intermediate class keeps a small bounded stack guarded by a spin lock. Renting happens on block cache
/// misses (which also do I/O), so the lock is never on a cache-hit path.</para>
/// </remarks>
internal static class BlockBufferPool
{
    private const int MinShift = 12;                  // classes start above 4 KiB
    private const int MaxShift = 20;                  // ... up to 1 MiB
    private const int ClassesPerDoubling = 4;
    private const int NumClasses = (MaxShift - MinShift) * ClassesPerDoubling;
    private const int MaxRetainedPerClass = 64;

    private sealed class Bucket
    {
        public readonly byte[]?[] Items = new byte[MaxRetainedPerClass][];
        public int Count;
        public int Lock;
    }

    private static readonly Bucket[] s_buckets = CreateBuckets();

    private static Bucket[] CreateBuckets()
    {
        var b = new Bucket[NumClasses];
        for (int i = 0; i < b.Length; i++) b[i] = new Bucket();
        return b;
    }

    /// <summary>The array length <see cref="Rent"/> returns for <paramref name="minimumLength"/>.</summary>
    public static int RoundUp(int minimumLength)
    {
        if (minimumLength <= 1 << MinShift || minimumLength > 1 << MaxShift) return minimumLength <= 16 ? 16 : (int)BitOperations.RoundUpToPowerOf2((uint)minimumLength);
        int e = BitOperations.Log2((uint)minimumLength - 1);      // 2^e < n <= 2^(e+1)
        int step = 1 << (e - 2);
        return ((minimumLength + step - 1) >> (e - 2)) << (e - 2);
    }

    /// <summary>Class index for an exact class size, or -1 (power of two or out of range: handled by the shared pool).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ClassIndex(int length)
    {
        if (length <= 1 << MinShift || length > 1 << MaxShift || BitOperations.IsPow2(length)) return -1;
        int e = BitOperations.Log2((uint)length);                 // 2^e < length < 2^(e+1)
        int stepShift = e - 2;
        if ((length & ((1 << stepShift) - 1)) != 0) return -1;    // not a class size
        int k = (length >> stepShift) - ClassesPerDoubling;       // 1..3
        return (e - MinShift) * ClassesPerDoubling + k;
    }

    /// <summary>Rents an array of at least <paramref name="minimumLength"/> bytes (length = <see cref="RoundUp"/>).</summary>
    public static byte[] Rent(int minimumLength)
    {
        int size = RoundUp(minimumLength);
        int idx = ClassIndex(size);
        if (idx < 0) return ArrayPool<byte>.Shared.Rent(minimumLength);
        Bucket b = s_buckets[idx];
        byte[]? array = null;
        Enter(ref b.Lock);
        if (b.Count > 0)
        {
            array = b.Items[--b.Count];
            b.Items[b.Count] = null;
        }
        Volatile.Write(ref b.Lock, 0);
        return array ?? GC.AllocateUninitializedArray<byte>(size);
    }

    /// <summary>Returns an array obtained from <see cref="Rent"/> or from <see cref="ArrayPool{T}.Shared"/>.</summary>
    public static void Return(byte[] array)
    {
        int idx = ClassIndex(array.Length);
        if (idx < 0)
        {
            if (BitOperations.IsPow2(array.Length) && array.Length >= 16) ArrayPool<byte>.Shared.Return(array);
            return;
        }
        Bucket b = s_buckets[idx];
        Enter(ref b.Lock);
        if (b.Count < MaxRetainedPerClass) b.Items[b.Count++] = array;
        Volatile.Write(ref b.Lock, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Enter(ref int l)
    {
        if (Interlocked.CompareExchange(ref l, 1, 0) != 0) EnterSlow(ref l);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void EnterSlow(ref int l)
    {
        SpinWait sw = default;
        do
        {
            sw.SpinOnce(sleep1Threshold: -1);
        } while (Volatile.Read(ref l) != 0 || Interlocked.CompareExchange(ref l, 1, 0) != 0);
    }
}
