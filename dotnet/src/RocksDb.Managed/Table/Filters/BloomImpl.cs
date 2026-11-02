using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Filters;

/// <summary>Port of BloomMath (util/bloom_impl.h): FP-rate estimates used to size filters.</summary>
public static class BloomMath
{
    /// <summary>FP rate of a standard (non-local) Bloom filter.</summary>
    public static double StandardFpRate(double bitsPerKey, int numProbes)
        => Math.Pow(1.0 - Math.Exp(-numProbes / bitsPerKey), numProbes);

    /// <summary>FP rate of a cache-local Bloom filter (accounts for uneven cache line loading).</summary>
    public static double CacheLocalFpRate(double bitsPerKey, int numProbes, int cacheLineBits)
    {
        if (bitsPerKey <= 0.0)
        {
            // Fix a discontinuity
            return 1.0;
        }
        double keysPerCacheLine = cacheLineBits / bitsPerKey;
        // A reasonable estimate is the average of the FP rates for one standard
        // deviation above and below the mean bucket occupancy.
        double keysStddev = Math.Sqrt(keysPerCacheLine);
        double crowdedFp = StandardFpRate(cacheLineBits / (keysPerCacheLine + keysStddev), numProbes);
        double uncrowdedFp = StandardFpRate(cacheLineBits / (keysPerCacheLine - keysStddev), numProbes);
        return (crowdedFp + uncrowdedFp) / 2;
    }

    /// <summary>FP rate from fingerprint (hash) collisions alone.</summary>
    public static double FingerprintFpRate(ulong numKeys, int fingerprintBits)
    {
        double invFingerprintSpace = Math.Pow(0.5, fingerprintBits);
        double baseEstimate = numKeys * invFingerprintSpace;
        if (baseEstimate > 0.0001)
        {
            return 1.0 - Math.Exp(-baseEstimate);
        }
        return baseEstimate - (baseEstimate * baseEstimate * 0.5);
    }

    /// <summary>Combines two independent FP rates.</summary>
    public static double IndependentProbabilitySum(double rate1, double rate2) => rate1 + rate2 - (rate1 * rate2);
}

/// <summary>
/// Port of FastLocalBloomImpl (util/bloom_impl.h): the format_version=5 Bloom filter. Each key maps to a
/// 64-byte cache line (by the lower 32 bits of the 64-bit hash) and probes bits within it using the
/// upper 32 bits. Persistent format.
/// </summary>
public static class FastLocalBloomImpl
{
    public static double EstimatedFpRate(ulong keys, ulong bytes, int numProbes, int hashBits)
        => BloomMath.IndependentProbabilitySum(
            BloomMath.CacheLocalFpRate(8.0 * bytes / keys, numProbes, /*cache line bits*/ 512),
            BloomMath.FingerprintFpRate(keys, hashBits));

    /// <summary>Optimal number of probes for the given millibits/key (schema-relevant: stored in metadata).</summary>
    public static int ChooseNumProbes(int millibitsPerKey)
    {
        // Since this implementation can (with AVX2) make up to 8 probes
        // for the same cost, we pick the most accurate num_probes, based
        // on actual tests of the implementation. Note that for higher
        // bits/key, the best choice for cache-local Bloom can be notably
        // smaller than standard bloom, e.g. 9 instead of 11 @ 16 b/k.
        if (millibitsPerKey <= 2080) return 1;
        if (millibitsPerKey <= 3580) return 2;
        if (millibitsPerKey <= 5100) return 3;
        if (millibitsPerKey <= 6640) return 4;
        if (millibitsPerKey <= 8300) return 5;
        if (millibitsPerKey <= 10070) return 6;
        if (millibitsPerKey <= 11720) return 7;
        if (millibitsPerKey <= 14001) return 8; // Would be something like <= 13800 but sacrificing *slightly* for more settings using <= 8 probes.
        if (millibitsPerKey <= 16050) return 9;
        if (millibitsPerKey <= 18300) return 10;
        if (millibitsPerKey <= 22001) return 11;
        if (millibitsPerKey <= 25501) return 12;
        if (millibitsPerKey > 50000) return 24; // Top out at 24 probes (three sets of 8)
        // Roughly optimal choices for remaining range
        // e.g.
        // 28000 -> 12, 28001 -> 13
        // 50000 -> 23, 50001 -> 24
        return ((millibitsPerKey - 1) / 2000) - 1;
    }

    /// <summary>Byte offset of the cache line selected by <paramref name="h1"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint CacheLineOffset(uint h1, uint lenBytes) => Hash.FastRange32(h1, lenBytes >> 6) << 6;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void AddHash(uint h1, uint h2, uint lenBytes, int numProbes, Span<byte> data)
        => AddHashPrepared(h2, numProbes, data.Slice((int)CacheLineOffset(h1, lenBytes), 64));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void AddHashPrepared(uint h2, int numProbes, Span<byte> dataAtCacheLine)
    {
        ref byte line = ref dataAtCacheLine[63]; // bounds check once
        line = ref Unsafe.Subtract(ref line, 63);
        uint h = h2;
        for (int i = 0; i < numProbes; ++i, h *= 0x9e3779b9u)
        {
            // 9-bit address within 512 bit cache line
            int bitpos = (int)(h >> (32 - 9));
            Unsafe.Add(ref line, bitpos >> 3) |= (byte)(1 << (bitpos & 7));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HashMayMatch(uint h1, uint h2, uint lenBytes, int numProbes, ReadOnlySpan<byte> data)
        => HashMayMatchPrepared(h2, numProbes, data.Slice((int)CacheLineOffset(h1, lenBytes), 64));

    /// <summary>Scalar probe loop (the C++ AVX2 path is an equivalent optimization).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HashMayMatchPrepared(uint h2, int numProbes, ReadOnlySpan<byte> dataAtCacheLine)
    {
        ref byte line = ref Unsafe.AsRef(in dataAtCacheLine[63]);
        line = ref Unsafe.Subtract(ref line, 63);
        uint h = h2;
        for (int i = 0; i < numProbes; ++i, h *= 0x9e3779b9u)
        {
            int bitpos = (int)(h >> (32 - 9));
            if ((Unsafe.Add(ref line, bitpos >> 3) & (1 << (bitpos & 7))) == 0)
            {
                return false;
            }
        }
        return true;
    }
}

/// <summary>Port of LegacyNoLocalityBloomImpl (util/bloom_impl.h): only ChooseNumProbes is still used (legacy filter sizing).</summary>
public static class LegacyNoLocalityBloomImpl
{
    public static int ChooseNumProbes(int bitsPerKey)
    {
        // We intentionally round down to reduce probing cost a little bit
        int numProbes = (int)(bitsPerKey * 0.69); // 0.69 =~ ln(2)
        if (numProbes < 1) numProbes = 1;
        if (numProbes > 30) numProbes = 30;
        return numProbes;
    }
}

/// <summary>
/// Port of LegacyLocalityBloomImpl&lt;ExtraRotates=false&gt; (util/bloom_impl.h): the format_version &lt; 5
/// full-filter Bloom implementation (32-bit <see cref="Hash.BloomHash"/>). Persistent format.
/// </summary>
public static class LegacyLocalityBloomImpl
{
    public static double EstimatedFpRate(ulong keys, ulong bytes, int numProbes)
    {
        double bitsPerKey = 8.0 * bytes / keys;
        double filterRate = BloomMath.CacheLocalFpRate(bitsPerKey, numProbes, /*cache line bits*/ 512);
        // ExtraRotates == false: correction for the (non-ideal) legacy probing
        filterRate += 0.1 / ((bitsPerKey * 0.75) + 22);
        double fingerprintRate = BloomMath.FingerprintFpRate(keys, 32);
        return BloomMath.IndependentProbabilitySum(filterRate, fingerprintRate);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint GetLineOffset(uint h, uint numLines, int log2CacheLineBytes) => (h % numLines) << log2CacheLineBytes;

    public static void AddHash(uint h, uint numLines, int numProbes, Span<byte> data, int log2CacheLineBytes)
    {
        int log2CacheLineBits = log2CacheLineBytes + 3;
        Span<byte> dataAtOffset = data.Slice((int)GetLineOffset(h, numLines, log2CacheLineBytes), 1 << log2CacheLineBytes);
        uint delta = (h >> 17) | (h << 15);
        uint mask = (1u << log2CacheLineBits) - 1;
        for (int i = 0; i < numProbes; ++i)
        {
            // Mimic the ExtraRotates=false behaviour: the low bits of h select the bit within the line.
            uint bitpos = h & mask;
            dataAtOffset[(int)(bitpos / 8)] |= (byte)(1 << (int)(bitpos % 8));
            h += delta;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool HashMayMatch(uint h, uint numLines, int numProbes, ReadOnlySpan<byte> data, int log2CacheLineBytes)
        => HashMayMatchPrepared(h, numProbes, data.Slice((int)GetLineOffset(h, numLines, log2CacheLineBytes), 1 << log2CacheLineBytes), log2CacheLineBytes);

    public static bool HashMayMatchPrepared(uint h, int numProbes, ReadOnlySpan<byte> dataAtOffset, int log2CacheLineBytes)
    {
        int log2CacheLineBits = log2CacheLineBytes + 3;
        uint delta = (h >> 17) | (h << 15);
        uint mask = (1u << log2CacheLineBits) - 1;
        for (int i = 0; i < numProbes; ++i)
        {
            uint bitpos = h & mask;
            if ((dataAtOffset[(int)(bitpos / 8)] & (1 << (int)(bitpos % 8))) == 0)
            {
                return false;
            }
            h += delta;
        }
        return true;
    }
}
