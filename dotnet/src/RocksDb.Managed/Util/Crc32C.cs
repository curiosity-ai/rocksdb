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

    /// <summary>Bytes per stream per round of the 3-way interleaved loop.</summary>
    private const int StripeBytes = 256;

    private static readonly bool s_hardware = System.Runtime.Intrinsics.X86.Sse42.X64.IsSupported || System.Runtime.Intrinsics.Arm.Crc32.Arm64.IsSupported;

    /// <summary>
    /// Byte-sliced table of the linear map "advance a raw CRC register over <see cref="StripeBytes"/> zero bytes"
    /// (4 x 256 entries), used to combine the three interleaved streams.
    /// </summary>
    private static readonly uint[] s_shiftTable = BuildShiftTable();

    private static uint[] BuildShiftTable()
    {
        var t = new uint[4 * 256];
        for (int b = 0; b < 4; b++)
        {
            for (uint v = 0; v < 256; v++)
            {
                uint reg = v << (8 * b);
                for (int i = 0; i < StripeBytes / 8; i++) reg = BitOperations.Crc32C(reg, 0UL);
                t[b * 256 + (int)v] = reg;
            }
        }
        return t;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ShiftStripe(uint[] t, uint crc) =>
        t[(byte)crc] ^ t[256 + (byte)(crc >> 8)] ^ t[512 + (byte)(crc >> 16)] ^ t[768 + (int)(crc >> 24)];

    /// <summary>Returns crc32c(init_crc || data). Matches crc32c::Extend.</summary>
    public static uint Extend(uint initCrc, ReadOnlySpan<byte> data)
    {
        uint crc = ~initCrc;
        // BitOperations.Crc32C is hardware accelerated on x86 (SSE4.2) and Arm64 (CRC32); on other
        // platforms it falls back to a software implementation internally.
        ref byte p = ref MemoryMarshal.GetReference(data);
        nuint len = (nuint)data.Length;
        nuint i = 0;
        if (len >= 3 * StripeBytes && s_hardware)
        {
            // Like util/crc32c.cc (crc32c_3way): the crc32 instruction has a latency of ~3 cycles but a throughput of
            // 1/cycle, so three independent streams run ~3x faster. The raw register update is affine, so
            // crc(A||B||C) = shift(shift(crcA) ^ crcB) ^ crcC with shift = "advance over StripeBytes zero bytes".
            uint[] t = s_shiftTable;
            do
            {
                uint a = crc, b = 0, c = 0;
                ref byte pa = ref Unsafe.Add(ref p, i);
                for (nuint j = 0; j < StripeBytes; j += 8)
                {
                    a = BitOperations.Crc32C(a, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref pa, j)));
                    b = BitOperations.Crc32C(b, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref pa, j + StripeBytes)));
                    c = BitOperations.Crc32C(c, Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref pa, j + 2 * StripeBytes)));
                }
                crc = ShiftStripe(t, ShiftStripe(t, a) ^ b) ^ c;
                i += 3 * StripeBytes;
            } while (len - i >= 3 * StripeBytes);
        }
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
