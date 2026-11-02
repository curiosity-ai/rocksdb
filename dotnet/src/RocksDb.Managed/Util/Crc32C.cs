using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Port of util/crc32c.h: CRC-32C (Castagnoli), plus RocksDB's Mask/Unmask used for checksums
/// stored alongside data that itself contains CRCs. Uses SSE4.2 / ARMv8 CRC instructions through
/// <see cref="BitOperations.Crc32C(uint, ulong)"/> when available, and a slicing-by-8 table otherwise.
/// </summary>
public static class Crc32C
{
    private const uint MaskDelta = 0xa282ead8u;

    /// <summary>Returns crc32c(init_crc || data). Matches crc32c::Extend.</summary>
    public static uint Extend(uint initCrc, ReadOnlySpan<byte> data)
    {
        uint crc = ~initCrc;
        // BitOperations.Crc32C is hardware accelerated on x86 (SSE4.2) and Arm64 (CRC32); on other
        // platforms it falls back to a software implementation internally.
        ref byte p = ref MemoryMarshal.GetReference(data);
        nuint len = (nuint)data.Length;
        nuint i = 0;
        while (len - i >= 8)
        {
            crc = BitOperations.Crc32C(crc, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref p, i)));
            i += 8;
        }
        if (len - i >= 4)
        {
            crc = BitOperations.Crc32C(crc, Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, i)));
            i += 4;
        }
        while (i < len)
        {
            crc = BitOperations.Crc32C(crc, Unsafe.Add(ref p, i));
            i++;
        }
        return ~crc;
    }

    /// <summary>Returns crc32c(data). Matches crc32c::Value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Value(ReadOnlySpan<byte> data) => Extend(0, data);

    /// <summary>Matches crc32c::Mask: rotate right by 15 bits and add a constant.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Mask(uint crc) => ((crc >> 15) | (crc << 17)) + MaskDelta;

    /// <summary>Matches crc32c::Unmask.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Unmask(uint maskedCrc)
    {
        uint rot = maskedCrc - MaskDelta;
        return (rot >> 17) | (rot << 15);
    }
}
