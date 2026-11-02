using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Port of util/xxph3.h: the frozen "XXH3 preview" (xxHash 0.7.2, renamed XXPH3) that backs
/// RocksDB's <c>Hash64</c> / <c>GetSliceHash64</c>, including RocksDB's modification for the empty
/// input (returns a hash of the seed instead of a constant). This is a persistent format (Bloom and
/// Ribbon filters hash keys with it) and differs from the released XXH3 (System.IO.Hashing.XxHash3)
/// for several length classes, so it must not be replaced with the library version.
/// Only the scalar code path is ported; results are identical on every platform by construction
/// (all multi-byte reads are little-endian).
/// </summary>
internal static class Xxph3
{
    private const uint Prime32_1 = 0x9E3779B1U;
    private const uint Prime32_2 = 0x85EBCA77U;
    private const uint Prime32_3 = 0xC2B2AE3DU;
    private const ulong Prime64_1 = 0x9E3779B185EBCA87UL;
    private const ulong Prime64_2 = 0xC2B2AE3D27D4EB4FUL;
    private const ulong Prime64_3 = 0x165667B19E3779F9UL;
    private const ulong Prime64_4 = 0x85EBCA77C2B2AE63UL;
    private const ulong Prime64_5 = 0x27D4EB2F165667C5UL;

    private const int StripeLen = 64;
    private const int SecretConsumeRate = 8;
    private const int AccNb = StripeLen / sizeof(ulong);
    private const int SecretDefaultSize = 192;
    private const int SecretSizeMin = 136;
    private const int MidSizeMax = 240;
    private const int MidSizeStartOffset = 3;
    private const int MidSizeLastOffset = 17;
    private const int SecretLastAccStart = 7;
    private const int SecretMergeAccsStart = 11;

    /// <summary>kSecret from xxph3.h (192 bytes).</summary>
    private static ReadOnlySpan<byte> KSecret =>
    [
        0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
        0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
        0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
        0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
        0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
        0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
        0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
        0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
        0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
        0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
        0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
        0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
    ];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Read64(ref byte p, int offset)
    {
        ulong v = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref p, offset));
        return BitConverter.IsLittleEndian ? v : BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Read32(ref byte p, int offset)
    {
        uint v = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, offset));
        return BitConverter.IsLittleEndian ? v : BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Mul128Fold64(ulong lhs, ulong rhs)
    {
        ulong hi = Math.BigMul(lhs, rhs, out ulong lo);
        return lo ^ hi;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Avalanche(ulong h64)
    {
        h64 ^= h64 >> 37;
        h64 *= Prime64_3;
        h64 ^= h64 >> 32;
        return h64;
    }

    /// <summary>XXPH3_64bits_withSeed (seed 0 == XXPH3_64bits).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(ReadOnlySpan<byte> input, ulong seed)
    {
        ref byte secret = ref MemoryMarshal.GetReference(KSecret);
        ref byte p = ref MemoryMarshal.GetReference(input);
        int len = input.Length;
        if (len <= 16) return Len0To16(ref p, len, ref secret, seed);
        if (len <= 128) return Len17To128(ref p, len, ref secret, seed);
        return HashLarger(input, seed);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ulong HashLarger(ReadOnlySpan<byte> input, ulong seed)
    {
        ref byte secret = ref MemoryMarshal.GetReference(KSecret);
        ref byte p = ref MemoryMarshal.GetReference(input);
        int len = input.Length;
        if (len <= MidSizeMax) return Len129To240(ref p, len, ref secret, seed);
        if (seed == 0) return HashLongInternal(ref p, len, ref secret, SecretDefaultSize);
        Span<byte> custom = stackalloc byte[SecretDefaultSize];
        InitCustomSecret(custom, seed);
        return HashLongInternal(ref p, len, ref MemoryMarshal.GetReference(custom), SecretDefaultSize);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Len0To16(ref byte input, int len, ref byte secret, ulong seed)
    {
        if (len > 8)
        {
            // XXPH3_len_9to16_64b
            ulong inputLo = Read64(ref input, 0) ^ (Read64(ref secret, 0) + seed);
            ulong inputHi = Read64(ref input, len - 8) ^ (Read64(ref secret, 8) - seed);
            ulong acc = (ulong)len + (inputLo + inputHi) + Mul128Fold64(inputLo, inputHi);
            return Avalanche(acc);
        }
        if (len >= 4)
        {
            // XXPH3_len_4to8_64b
            uint inputLo = Read32(ref input, 0);
            uint inputHi = Read32(ref input, len - 4);
            ulong input64 = inputLo | ((ulong)inputHi << 32);
            ulong keyed = input64 ^ (Read64(ref secret, 0) + seed);
            ulong mix64 = (ulong)len + ((keyed ^ (keyed >> 51)) * Prime32_1);
            return Avalanche((mix64 ^ (mix64 >> 47)) * Prime64_2);
        }
        if (len > 0)
        {
            // XXPH3_len_1to3_64b
            byte c1 = input;
            byte c2 = Unsafe.Add(ref input, len >> 1);
            byte c3 = Unsafe.Add(ref input, len - 1);
            uint combined = c1 | ((uint)c2 << 8) | ((uint)c3 << 16) | ((uint)len << 24);
            ulong keyed = combined ^ (Read32(ref secret, 0) + seed);
            ulong mixed = keyed * Prime64_1;
            return Avalanche(mixed);
        }
        // RocksDB modification from XXPH3 preview: hash of the seed for empty input.
        return Mul128Fold64(seed + Read64(ref secret, 0), Prime64_2);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Mix16B(ref byte input, int inOff, ref byte secret, int secOff, ulong seed)
    {
        ulong inputLo = Read64(ref input, inOff);
        ulong inputHi = Read64(ref input, inOff + 8);
        return Mul128Fold64(inputLo ^ (Read64(ref secret, secOff) + seed),
                            inputHi ^ (Read64(ref secret, secOff + 8) - seed));
    }

    private static ulong Len17To128(ref byte input, int len, ref byte secret, ulong seed)
    {
        ulong acc = (ulong)len * Prime64_1;
        if (len > 32)
        {
            if (len > 64)
            {
                if (len > 96)
                {
                    acc += Mix16B(ref input, 48, ref secret, 96, seed);
                    acc += Mix16B(ref input, len - 64, ref secret, 112, seed);
                }
                acc += Mix16B(ref input, 32, ref secret, 64, seed);
                acc += Mix16B(ref input, len - 48, ref secret, 80, seed);
            }
            acc += Mix16B(ref input, 16, ref secret, 32, seed);
            acc += Mix16B(ref input, len - 32, ref secret, 48, seed);
        }
        acc += Mix16B(ref input, 0, ref secret, 0, seed);
        acc += Mix16B(ref input, len - 16, ref secret, 16, seed);
        return Avalanche(acc);
    }

    private static ulong Len129To240(ref byte input, int len, ref byte secret, ulong seed)
    {
        ulong acc = (ulong)len * Prime64_1;
        int nbRounds = len / 16;
        for (int i = 0; i < 8; i++)
        {
            acc += Mix16B(ref input, 16 * i, ref secret, 16 * i, seed);
        }
        acc = Avalanche(acc);
        for (int i = 8; i < nbRounds; i++)
        {
            acc += Mix16B(ref input, 16 * i, ref secret, (16 * (i - 8)) + MidSizeStartOffset, seed);
        }
        // last bytes
        acc += Mix16B(ref input, len - 16, ref secret, SecretSizeMin - MidSizeLastOffset, seed);
        return Avalanche(acc);
    }

    private static void InitCustomSecret(Span<byte> customSecret, ulong seed64)
    {
        ReadOnlySpan<byte> k = KSecret;
        const int nbRounds = SecretDefaultSize / 16;
        for (int i = 0; i < nbRounds; i++)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(customSecret[(16 * i)..], BinaryPrimitives.ReadUInt64LittleEndian(k[(16 * i)..]) + seed64);
            BinaryPrimitives.WriteUInt64LittleEndian(customSecret[((16 * i) + 8)..], BinaryPrimitives.ReadUInt64LittleEndian(k[((16 * i) + 8)..]) - seed64);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Accumulate512(Span<ulong> acc, ref byte input, int inOff, ref byte secret, int secOff)
    {
        // Scalar variant, XXPH3_acc_64bits
        for (int i = 0; i < AccNb; i++)
        {
            ulong dataVal = Read64(ref input, inOff + (8 * i));
            ulong dataKey = dataVal ^ Read64(ref secret, secOff + (i * 8));
            acc[i] += dataVal;
            acc[i] += (dataKey & 0xFFFFFFFFUL) * (dataKey >> 32);
        }
    }

    private static void ScrambleAcc(Span<ulong> acc, ref byte secret, int secOff)
    {
        for (int i = 0; i < AccNb; i++)
        {
            ulong key64 = Read64(ref secret, secOff + (8 * i));
            ulong acc64 = acc[i];
            acc64 ^= acc64 >> 47;
            acc64 ^= key64;
            acc64 *= Prime32_1;
            acc[i] = acc64;
        }
    }

    private static void Accumulate(Span<ulong> acc, ref byte input, int inOff, ref byte secret, int nbStripes)
    {
        for (int n = 0; n < nbStripes; n++)
        {
            Accumulate512(acc, ref input, inOff + (n * StripeLen), ref secret, n * SecretConsumeRate);
        }
    }

    private static ulong HashLongInternal(ref byte input, int len, ref byte secret, int secretSize)
    {
        Span<ulong> acc = stackalloc ulong[AccNb];
        acc[0] = Prime32_3; acc[1] = Prime64_1; acc[2] = Prime64_2; acc[3] = Prime64_3;
        acc[4] = Prime64_4; acc[5] = Prime32_2; acc[6] = Prime64_5; acc[7] = Prime32_1;

        // XXPH3_hashLong_internal_loop
        int nbRounds = (secretSize - StripeLen) / SecretConsumeRate;
        int blockLen = StripeLen * nbRounds;
        int nbBlocks = len / blockLen;
        for (int n = 0; n < nbBlocks; n++)
        {
            Accumulate(acc, ref input, n * blockLen, ref secret, nbRounds);
            ScrambleAcc(acc, ref secret, secretSize - StripeLen);
        }
        // last partial block
        int nbStripes = (len - (blockLen * nbBlocks)) / StripeLen;
        Accumulate(acc, ref input, nbBlocks * blockLen, ref secret, nbStripes);
        // last stripe
        if ((len & (StripeLen - 1)) != 0)
        {
            Accumulate512(acc, ref input, len - StripeLen, ref secret, secretSize - StripeLen - SecretLastAccStart);
        }

        // XXPH3_mergeAccs
        ulong result64 = (ulong)len * Prime64_1;
        for (int i = 0; i < 4; i++)
        {
            int off = SecretMergeAccsStart + (16 * i);
            result64 += Mul128Fold64(acc[2 * i] ^ Read64(ref secret, off), acc[(2 * i) + 1] ^ Read64(ref secret, off + 8));
        }
        return Avalanche(result64);
    }
}
