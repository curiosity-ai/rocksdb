using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Port of util/hash.h and util/hash.cc (plus util/fastrange.h). Several of these hashes are part of
/// persisted formats (Bloom/Ribbon filters, data block hash index, ...), so their values must match
/// upstream bit for bit.
/// <list type="bullet">
/// <item><see cref="Hash32(ReadOnlySpan{byte}, uint)"/> (C++ <c>Hash()</c>): MurmurHash1 variant (legacy Bloom filter, data block hash index).</item>
/// <item><see cref="Hash64(ReadOnlySpan{byte}, ulong)"/>: the frozen XXH3 preview (<see cref="Xxph3"/>), used by
///   format_version=5 Bloom and Ribbon filters via <see cref="GetSliceHash64"/>.</item>
/// <item><see cref="Hash2x64(ReadOnlySpan{byte}, ulong, out ulong, out ulong)"/>: released XXH3 128-bit
///   (identical to <see cref="XxHash128"/>).</item>
/// </list>
/// </summary>
public static class Hash
{
    /// <summary>Seed used by <see cref="BloomHash"/> (util/hash.h).</summary>
    public const uint BloomHashSeed = 0xbc9f1d34;

    /// <summary>Seed used by <see cref="GetSliceHash"/> (util/hash.h).</summary>
    public const uint SliceHashSeed = 397;

    /// <summary>
    /// <c>Hash(data, n, seed)</c> from util/hash.cc: MurmurHash1-like 32-bit hash. Trailing bytes are
    /// sign-extended (historic behaviour of the C++ code with signed char; part of the format).
    /// </summary>
    public static uint Hash32(ReadOnlySpan<byte> data, uint seed)
    {
        const uint m = 0xc6a4a793;
        const int r = 24;
        int n = data.Length;
        uint h = seed ^ unchecked((uint)n * m);

        int i = 0;
        // Pick up four bytes at a time
        while (n - i >= 4)
        {
            uint w = BinaryPrimitives.ReadUInt32LittleEndian(data[i..]);
            i += 4;
            h += w;
            h *= m;
            h ^= h >> 16;
        }

        // Pick up remaining bytes (sign extension via sbyte matches the C++ int8_t casts)
        switch (n - i)
        {
            case 3:
                h += (uint)(sbyte)data[i + 2] << 16;
                goto case 2;
            case 2:
                h += (uint)(sbyte)data[i + 1] << 8;
                goto case 1;
            case 1:
                h += (uint)(sbyte)data[i];
                h *= m;
                h ^= h >> r;
                break;
        }
        return h;
    }

    /// <summary>BloomHash(key): hash used by the legacy (format_version &lt; 5) Bloom filter.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint BloomHash(ReadOnlySpan<byte> key) => Hash32(key, BloomHashSeed);

    /// <summary>GetSliceHash(s) = Hash(s, 397).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint GetSliceHash(ReadOnlySpan<byte> s) => Hash32(s, SliceHashSeed);

    /// <summary>Hash64(data, n, seed) = XXPH3_64bits_withSeed (stable, persisted).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(ReadOnlySpan<byte> data, ulong seed) => Xxph3.Hash64(data, seed);

    /// <summary>Hash64(data, n) = XXPH3_64bits (seed 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Hash64(ReadOnlySpan<byte> data) => Xxph3.Hash64(data, 0);

    /// <summary>GetSliceHash64(key): the 64-bit key hash used by Bloom (format_version=5) and Ribbon filters.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetSliceHash64(ReadOnlySpan<byte> key) => Xxph3.Hash64(key, 0);

    /// <summary>
    /// NPHash64: "non-persistent" 64-bit hash, currently the same as <see cref="Hash64(ReadOnlySpan{byte}, ulong)"/>
    /// (upstream reserves the right to change it, so never persist its values).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong NPHash64(ReadOnlySpan<byte> data, ulong seed) => Xxph3.Hash64(data, seed);

    /// <summary>GetSliceNPHash64(s) (seed 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetSliceNPHash64(ReadOnlySpan<byte> s) => Xxph3.Hash64(s, 0);

    /// <summary>GetSliceNPHash64(s, seed).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetSliceNPHash64(ReadOnlySpan<byte> s, ulong seed) => Xxph3.Hash64(s, seed);

    /// <summary>GetSliceRangedNPHash(s, range) = FastRange64(NPHash64(s), range).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetSliceRangedNPHash(ReadOnlySpan<byte> s, ulong range) => FastRange64(Xxph3.Hash64(s, 0), range);

    /// <summary>Hash128(data, n, seed) = XXH3_128bits_withSeed (released XXH3; identical to <see cref="XxHash128"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static UInt128 Hash128(ReadOnlySpan<byte> data, ulong seed) => XxHash128.HashToUInt128(data, unchecked((long)seed));

    /// <summary>GetSliceHash128(s) = Hash128(s, 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static UInt128 GetSliceHash128(ReadOnlySpan<byte> s) => XxHash128.HashToUInt128(s, 0);

    /// <summary>Hash2x64(data, n, seed, &amp;high64, &amp;low64) (XXH3 128-bit).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Hash2x64(ReadOnlySpan<byte> data, ulong seed, out ulong high64, out ulong low64)
    {
        UInt128 h = XxHash128.HashToUInt128(data, unchecked((long)seed));
        high64 = (ulong)(h >> 64);
        low64 = (ulong)h;
    }

    /// <summary>Hash2x64(data, n, &amp;high64, &amp;low64) (seed 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Hash2x64(ReadOnlySpan<byte> data, out ulong high64, out ulong low64) => Hash2x64(data, 0, out high64, out low64);

    /// <summary>GetSliceHash2x64 (seed 0).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GetSliceHash2x64(ReadOnlySpan<byte> s, out ulong high64, out ulong low64) => Hash2x64(s, 0, out high64, out low64);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Upper32of64(ulong v) => (uint)(v >> 32);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Lower32of64(ulong v) => (uint)v;

    /// <summary>XXH32 (seed 0 by default), as used by the kxxHash block checksum type.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint XxHash32Value(ReadOnlySpan<byte> data, uint seed) => XxHash32.HashToUInt32(data, unchecked((int)seed));

    /// <summary>XXH64, as used by the kxxHash64 block checksum type (which keeps the lower 32 bits).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong XxHash64Value(ReadOnlySpan<byte> data, ulong seed) => XxHash64.HashToUInt64(data, unchecked((long)seed));

    /// <summary>Released XXH3 64-bit (NOT the same as <see cref="Hash64(ReadOnlySpan{byte})"/>); used by the kXXH3 checksum type.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Xxh3Value(ReadOnlySpan<byte> data, ulong seed) => XxHash3.HashToUInt64(data, unchecked((long)seed));

    // ---- Bijective 2x64 hash (util/hash.cc) --------------------------------------------------

    private static ulong Xxh3Avalanche(ulong h64)
    {
        h64 ^= h64 >> 37;
        h64 *= 0x165667919E3779F9UL;
        h64 ^= h64 >> 32;
        return h64;
    }

    private static ulong Xxh3Unavalanche(ulong h64)
    {
        h64 ^= h64 >> 32;
        h64 *= 0x8da8ee41d6df849UL; // inverse of 0x165667919E3779F9U
        h64 ^= h64 >> 37;
        return h64;
    }

    /// <summary>
    /// BijectiveHash2x64: a bijection on 128-bit values that equals <see cref="Hash2x64(ReadOnlySpan{byte}, ulong, out ulong, out ulong)"/>
    /// of the 16-byte little-endian encoding of (in_low64, in_high64).
    /// </summary>
    public static void BijectiveHash2x64(ulong inHigh64, ulong inLow64, ulong seed, out ulong outHigh64, out ulong outLow64)
    {
        // Adapted from XXH3_len_9to16_128b
        ulong bitflipl = 0x59973f0033362349UL - seed;
        ulong bitfliph = 0xc202797692d63d58UL + seed;
        ulong hi = Math.BigMul(inLow64 ^ inHigh64 ^ bitflipl, 0x9E3779B185EBCA87UL, out ulong lo);
        lo += 0x3c0000000000000UL; // (len - 1) << 54
        inHigh64 ^= bitfliph;
        hi += inHigh64 + ((ulong)Lower32of64(inHigh64) * 0x85EBCA76UL);
        lo ^= BinaryPrimitives.ReverseEndianness(hi);
        ulong hi2 = Math.BigMul(lo, 0xC2B2AE3D27D4EB4FUL, out ulong lo2);
        hi = hi2 + (hi * 0xC2B2AE3D27D4EB4FUL);
        outLow64 = Xxh3Avalanche(lo2);
        outHigh64 = Xxh3Avalanche(hi);
    }

    public static void BijectiveHash2x64(ulong inHigh64, ulong inLow64, out ulong outHigh64, out ulong outLow64)
        => BijectiveHash2x64(inHigh64, inLow64, 0, out outHigh64, out outLow64);

    /// <summary>Inverse of <see cref="BijectiveHash2x64(ulong, ulong, ulong, out ulong, out ulong)"/>.</summary>
    public static void BijectiveUnhash2x64(ulong inHigh64, ulong inLow64, ulong seed, out ulong outHigh64, out ulong outLow64)
    {
        ulong bitflipl = 0x59973f0033362349UL - seed;
        ulong bitfliph = 0xc202797692d63d58UL + seed;
        ulong lo = Xxh3Unavalanche(inLow64);
        ulong hi = Xxh3Unavalanche(inHigh64);
        lo *= 0xba79078168d4bafUL; // inverse of 0xC2B2AE3D27D4EB4FU
        hi -= Math.BigMul(lo, 0xC2B2AE3D27D4EB4FUL, out _);
        hi *= 0xba79078168d4bafUL;
        lo ^= BinaryPrimitives.ReverseEndianness(hi);
        lo -= 0x3c0000000000000UL;
        lo *= 0x887493432badb37UL; // inverse of 0x9E3779B185EBCA87U
        hi -= Math.BigMul(lo, 0x9E3779B185EBCA87UL, out _);
        uint tmp32 = Lower32of64(hi) * 0xb6c92f47u; // inverse of 0x85EBCA77
        hi -= tmp32;
        hi = (hi & 0xFFFFFFFF00000000UL) - (((ulong)tmp32 * 0x85EBCA76UL) & 0xFFFFFFFF00000000UL) + tmp32;
        hi ^= bitfliph;
        lo ^= hi ^ bitflipl;
        outHigh64 = hi;
        outLow64 = lo;
    }

    public static void BijectiveUnhash2x64(ulong inHigh64, ulong inLow64, out ulong outHigh64, out ulong outLow64)
        => BijectiveUnhash2x64(inHigh64, inLow64, 0, out outHigh64, out outLow64);

    // ---- util/fastrange.h ---------------------------------------------------------------------

    /// <summary>FastRange32: maps a 32-bit hash uniformly onto [0, range).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint FastRange32(uint hash, uint range) => (uint)(((ulong)range * hash) >> 32);

    /// <summary>FastRange64: maps a 64-bit hash uniformly onto [0, range) (upper 64 bits of the 128-bit product).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong FastRange64(ulong hash, ulong range) => Math.BigMul(range, hash, out _);
}
