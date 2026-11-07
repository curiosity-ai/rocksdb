using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// memcmp-order comparison tuned for short keys (the common case for RocksDB keys: 8..64 bytes). Compares 8 bytes at
/// a time as big-endian integers and falls back to the vectorized
/// <see cref="MemoryExtensions.SequenceCompareTo{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/> for long common lengths.
/// Equivalent to <c>a.SequenceCompareTo(b)</c> in sign (the magnitude is unspecified: callers only test the sign).
/// </summary>
/// <remarks>
/// Rationale (docs/perf.md): block seeks, index seeks and skiplist searches spend much of their time comparing
/// 16..32-byte keys that share a long prefix; the inlined word loop avoids the call and the length dispatch of
/// <c>SpanHelpers.SequenceCompareTo</c>.
/// </remarks>
internal static class ByteCompare
{
    private const int InlineLimit = 64;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int la = a.Length, lb = b.Length;
        int n = la < lb ? la : lb;
        if (n > InlineLimit) return a.SequenceCompareTo(b);
        ref byte pa = ref MemoryMarshal.GetReference(a);
        ref byte pb = ref MemoryMarshal.GetReference(b);
        nuint i = 0;
        nuint end = (nuint)n;
        while (i + 8 <= end)
        {
            ulong x = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref pa, i));
            ulong y = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref pb, i));
            if (x != y) return CompareWords(x, y);
            i += 8;
        }
        if (i + 4 <= end)
        {
            uint x = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref pa, i));
            uint y = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref pb, i));
            if (x != y)
            {
                if (BitConverter.IsLittleEndian)
                {
                    x = BinaryPrimitives.ReverseEndianness(x);
                    y = BinaryPrimitives.ReverseEndianness(y);
                }
                return x < y ? -1 : 1;
            }
            i += 4;
        }
        while (i < end)
        {
            int d = Unsafe.Add(ref pa, i) - Unsafe.Add(ref pb, i);
            if (d != 0) return d;
            i++;
        }
        return la - lb;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareWords(ulong x, ulong y)
    {
        if (BitConverter.IsLittleEndian)
        {
            x = BinaryPrimitives.ReverseEndianness(x);
            y = BinaryPrimitives.ReverseEndianness(y);
        }
        return x < y ? -1 : 1;
    }
}
