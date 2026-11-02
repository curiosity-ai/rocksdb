using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.Filters;

/// <summary>
/// Port of the Standard128Ribbon instantiation used by RocksDB's Ribbon filter:
/// util/ribbon_impl.h (StandardHasher, StandardRehasherAdapter, StandardBanding,
/// SerializableInterleavedSolution), util/ribbon_alg.h (BandingAdd, BandingAddRange,
/// BackSubstBlock, InterleavedBackSubst, InterleavedPrepareQuery, InterleavedFilterQuery) and
/// util/ribbon_config.cc (BandingConfigHelper1 for kOneIn20 / 128 coefficient bits / no smash /
/// non-homogeneous), with the settings of Standard128RibbonRehasherTypesAndSettings in
/// table/block_based/filter_policy.cc:
/// kIsFilter=true, kHomogeneous=false, kFirstCoeffAlwaysOne=true, kUseSmash=false,
/// CoeffRow=Unsigned128, Hash=uint64, Seed=uint32, Index=uint32, ResultRow=uint32, kAllowZeroStarts=false.
/// 128-bit coefficient rows are represented as (lo, hi) pairs of ulong. Persistent format.
/// </summary>
internal static class Standard128Ribbon
{
    public const int CoeffBits = 128;
    public const int ResultBits = 32;
    public const int BytesPerSegment = 16; // sizeof(CoeffRow)

    // StandardHasher constants (util/ribbon_impl.h)
    private const ulong CoeffAndResultFactor = 0xc28f82822b650bedUL;
    private const ulong CoeffXor64 = 0xc367844a6e52731dUL;
    private const ulong SeedMixMask = 0xf0f0f0f0f0f0f0f0UL;
    private const int SeedMixShift = 4;
    private const ulong ToRawSeedFactor = 0xc78219a23eeadd03UL;
    private const ulong FromRawSeedFactor = 0xfe1a137d14b475abUL;
    // StandardRehasherAdapter
    private const ulong RehashFactor = 0x6193d459236a3a0dUL;

    // ---- Hasher -------------------------------------------------------------------------------

    /// <summary>StandardHasher::SetOrdinalSeed: maps ordinal seed (0..255 for the filter) to the raw seed.</summary>
    public static uint OrdinalToRawSeed(uint ordinal)
    {
        ulong tmp = ordinal * ToRawSeedFactor;
        tmp ^= (tmp & SeedMixMask) >> SeedMixShift;
        return (uint)tmp;
    }

    /// <summary>StandardHasher::GetOrdinalSeed.</summary>
    public static uint RawToOrdinalSeed(uint raw)
    {
        ulong tmp = raw;
        tmp ^= (tmp & SeedMixMask) >> SeedMixShift;
        return (uint)(tmp * FromRawSeedFactor);
    }

    /// <summary>GetHash = StandardRehasherAdapter::HashFn(key_hash, raw_seed).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Rehash(ulong keyHash, uint rawSeed) => (keyHash ^ rawSeed) * RehashFactor;

    /// <summary>GetStart (no smash): FastRangeGeneric(h, num_starts).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint GetStart(ulong h, uint numStarts) => (uint)Math.BigMul(h, numStarts, out _);

    /// <summary>GetCoeffRow for 64-bit Hash and 128-bit CoeffRow with kFirstCoeffAlwaysOne.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GetCoeffRow(ulong h, out ulong lo, out ulong hi)
    {
        ulong b = h * CoeffAndResultFactor;
        hi = b;
        lo = (b ^ CoeffXor64) | 1;
    }

    /// <summary>GetResultRowFromHash: lower 32 bits of the byte-swapped product.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint GetResultRowFromHash(ulong h) => (uint)BinaryPrimitives.ReverseEndianness(h * CoeffAndResultFactor);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Parity(ulong lo, ulong hi) => BitOperations.PopCount(lo ^ hi) & 1;

    // ---- Config (util/ribbon_config.cc, kOneIn20, 128-bit, no smash) ----------------------------

    private const int KnownSize = 18;

    private static ReadOnlySpan<double> KnownToAddByPow2 =>
    [
        0, 0, 0, 0, 0, 0, 0, 0, // unsupported
        248.851, 499.532, 1001.26, 2003.97, 4005.59, 8000.39, 15966.6, 31828.1, 63447.3, 126506,
    ];

    private const double FactorPerPow2 = 0.0038; // kCoeffBits == 128

    private static double GetFinalKnownFactor() => 1.0 * (1u << (KnownSize - 1)) / KnownToAddByPow2[KnownSize - 1];

    private static double GetBaseFactor() => GetFinalKnownFactor() - ((KnownSize - 1) * FactorPerPow2);

    private static double GetFactorForLarge(double log2NumSlots) => GetBaseFactor() + (log2NumSlots * FactorPerPow2);

    private static double GetNumToAddForPow2(uint log2NumSlots)
    {
        if (log2NumSlots < KnownSize)
        {
            return KnownToAddByPow2[(int)log2NumSlots];
        }
        return 1.0 * (1UL << (int)log2NumSlots) / GetFactorForLarge(1.0 * log2NumSlots);
    }

    /// <summary>BandingConfigHelper1::GetNumToAdd.</summary>
    public static uint GetNumToAdd(uint numSlots)
    {
        if (numSlots == 0) return 0;
        double log2NumSlots = Math.Log(numSlots) * 1.4426950409;
        uint floorLog2 = (uint)log2NumSlots;
        if (floorLog2 + 1 < KnownSize)
        {
            double ceilPortion = (1.0 * numSlots / (1u << (int)floorLog2)) - 1.0;
            return (uint)((ceilPortion * KnownToAddByPow2[(int)floorLog2 + 1]) + ((1.0 - ceilPortion) * KnownToAddByPow2[(int)floorLog2]));
        }
        double factor = GetFactorForLarge(log2NumSlots);
        return (uint)(numSlots / factor);
    }

    /// <summary>BandingConfigHelper1::GetNumSlots.</summary>
    public static uint GetNumSlots(uint numToAdd)
    {
        if (numToAdd == 0) return 0;
        double log2NumToAdd = Math.Log(numToAdd) * 1.4426950409;
        uint approxLog2Slots = (uint)(log2NumToAdd + 0.5);
        double lowerNumToAdd = GetNumToAddForPow2(approxLog2Slots);
        double upperNumToAdd;
        if (approxLog2Slots == 0 || lowerNumToAdd == /* unsupported */ 0)
        {
            // Return minimum non-zero slots in standard implementation
            return 2 * CoeffBits;
        }
        else if (numToAdd < lowerNumToAdd)
        {
            upperNumToAdd = lowerNumToAdd;
            --approxLog2Slots;
            lowerNumToAdd = GetNumToAddForPow2(approxLog2Slots);
        }
        else
        {
            upperNumToAdd = GetNumToAddForPow2(approxLog2Slots + 1);
        }
        double upperPortion = (numToAdd - lowerNumToAdd) / (upperNumToAdd - lowerNumToAdd);
        double lowerNumSlots = 1.0 * (1UL << (int)approxLog2Slots);
        // Interpolation, round up
        return (uint)((upperPortion * lowerNumSlots) + lowerNumSlots + 0.999999999);
    }

    // ---- SerializableInterleavedSolution static helpers ----------------------------------------

    public static uint RoundUpNumSlots(uint numSlots)
    {
        // Must be multiple of kCoeffBits; and num_starts == 1 not allowed (no smash)
        uint corrected = (numSlots + CoeffBits - 1) / CoeffBits * CoeffBits;
        if (corrected == CoeffBits) corrected += CoeffBits;
        return corrected;
    }

    public static uint RoundDownNumSlots(uint numSlots)
    {
        uint corrected = numSlots / CoeffBits * CoeffBits;
        if (corrected == CoeffBits) corrected = 0;
        return corrected;
    }

    /// <summary>SerializableInterleavedSolution::GetBytesForOneInFpRate.</summary>
    public static ulong GetBytesForOneInFpRate(uint numSlots, double desiredOneInFpRate, uint roundingBias32)
        => InternalGetBytesForFpRate(numSlots, 1.0 / desiredOneInFpRate, desiredOneInFpRate, roundingBias32);

    private static ulong InternalGetBytesForFpRate(uint numSlots, double desiredFpRate, double desiredOneInFpRate, uint roundingBias32)
    {
        if (desiredOneInFpRate > 1.0 && desiredFpRate < 1.0)
        {
            // Typical: less than 100% FP rate
            if (desiredOneInFpRate <= uint.MaxValue)
            {
                // Typical: Less than maximum result row entropy
                uint rounded = (uint)desiredOneInFpRate;
                int lowerColumns = BitOperations.Log2(rounded);
                double lowerColumnsFpRate = Math.Pow(2.0, -lowerColumns);
                double upperColumnsFpRate = Math.Pow(2.0, -(lowerColumns + 1));
                // Floating point don't let me down!
                double lowerPortion = (desiredFpRate - upperColumnsFpRate) / (lowerColumnsFpRate - upperColumnsFpRate);
                // Choose a rounding bias, using the provided 32-bit value
                double roundingBias = (roundingBias32 + 0.5) / 4294967296.0;
                uint numStarts = numSlots - CoeffBits + 1;
                // Lower columns fill in the first part of the solution
                uint upperStartBlock = (uint)(((lowerPortion * numStarts) + roundingBias) / CoeffBits);
                uint numBlocks = numSlots / CoeffBits;
                // Compute the number of 128-bit segments
                uint numSegments = numBlocks * (uint)lowerColumns;
                numSegments += numBlocks - upperStartBlock;
                return (ulong)numSegments * BytesPerSegment;
            }
            // one column of result row bits per slot
            return (ulong)numSlots * sizeof(uint);
        }
        // Must be 100% FP rate (or NaN): minimum non-empty solution
        return BytesPerSegment;
    }

    /// <summary>Solution layout derived from num_blocks and the data length (InternalConfigure).</summary>
    public readonly struct Layout
    {
        public readonly uint NumStarts;
        public readonly uint NumBlocks;
        public readonly uint UpperNumColumns;
        public readonly uint UpperStartBlock;
        public readonly uint NumSegments;

        public Layout(uint numBlocks, ulong dataLen)
        {
            NumBlocks = numBlocks;
            NumStarts = numBlocks == 0 ? 0 : (numBlocks * CoeffBits) - CoeffBits + 1;
            uint numSegments = (uint)Math.Min(dataLen / BytesPerSegment, uint.MaxValue);
            if (numBlocks == 0)
            {
                UpperNumColumns = 0;
                UpperStartBlock = 0;
            }
            else
            {
                // Largest number of columns, rounded up
                UpperNumColumns = (numSegments + numBlocks - 1) / numBlocks;
                // Index of first block using fewer columns
                UpperStartBlock = (UpperNumColumns * numBlocks) - numSegments;
                // Maybe uneven distribution, but we have cap on ResultRow bits
                if (UpperNumColumns > ResultBits)
                {
                    UpperNumColumns = ResultBits;
                    UpperStartBlock = 0;
                    numSegments = numBlocks * UpperNumColumns;
                }
            }
            NumSegments = numSegments;
        }

        /// <summary>SerializableInterleavedSolution::ExpectedFpRate.</summary>
        public double ExpectedFpRate()
        {
            double lowerPortion = UpperStartBlock * 1.0 * CoeffBits / NumStarts;
            return (lowerPortion * Math.Pow(0.5, UpperNumColumns - 1)) + ((1.0 - lowerPortion) * Math.Pow(0.5, UpperNumColumns));
        }
    }

    // ---- Query (InterleavedPrepareQuery + InterleavedFilterQuery) ------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void LoadSegment(ReadOnlySpan<byte> data, uint segmentNum, out ulong lo, out ulong hi)
    {
        ReadOnlySpan<byte> s = data.Slice((int)(segmentNum * BytesPerSegment), BytesPerSegment);
        lo = BinaryPrimitives.ReadUInt64LittleEndian(s);
        hi = BinaryPrimitives.ReadUInt64LittleEndian(s[8..]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ShiftLeft(ulong lo, ulong hi, int n, out ulong rlo, out ulong rhi)
    {
        // 0 <= n < 128
        if (n == 0) { rlo = lo; rhi = hi; }
        else if (n < 64) { rlo = lo << n; rhi = (hi << n) | (lo >> (64 - n)); }
        else { rlo = 0; rhi = lo << (n - 64); }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ShiftRight(ulong lo, ulong hi, int n, out ulong rlo, out ulong rhi)
    {
        // 0 < n <= 128 (n == 128 not used)
        if (n < 64) { rlo = (lo >> n) | (hi << (64 - n)); rhi = hi >> n; }
        else { rlo = hi >> (n - 64); rhi = 0; }
    }

    /// <summary>Ribbon filter query against serialized solution data (excluding metadata).</summary>
    public static bool FilterQuery(ulong keyHash, uint rawSeed, ReadOnlySpan<byte> data, in Layout layout)
    {
        ulong hash = Rehash(keyHash, rawSeed);
        uint startSlot = GetStart(hash, layout.NumStarts);
        uint upperStartBlock = layout.UpperStartBlock;
        uint numColumns = layout.UpperNumColumns;
        uint startBlockNum = startSlot / CoeffBits;
        uint segmentNum = (startBlockNum * numColumns) - Math.Min(startBlockNum, upperStartBlock);
        // change to lower num columns if applicable.
        numColumns -= startBlockNum < upperStartBlock ? 1u : 0u;
        int startBit = (int)(startSlot % CoeffBits);

        GetCoeffRow(hash, out ulong crLo, out ulong crHi);
        uint expected = GetResultRowFromHash(hash);

        if (startBit == 0)
        {
            for (uint i = 0; i < numColumns; ++i)
            {
                LoadSegment(data, segmentNum + i, out ulong sLo, out ulong sHi);
                if (Parity(sLo & crLo, sHi & crHi) != (int)((expected >> (int)i) & 1))
                {
                    return false;
                }
            }
        }
        else
        {
            ShiftLeft(crLo, crHi, startBit, out ulong lLo, out ulong lHi);
            ShiftRight(crLo, crHi, CoeffBits - startBit, out ulong rLo, out ulong rHi);
            for (uint i = 0; i < numColumns; ++i)
            {
                LoadSegment(data, segmentNum + i, out ulong aLo, out ulong aHi);
                LoadSegment(data, segmentNum + numColumns + i, out ulong bLo, out ulong bHi);
                ulong xLo = (aLo & lLo) ^ (bLo & rLo);
                ulong xHi = (aHi & lHi) ^ (bHi & rHi);
                if (Parity(xLo, xHi) != (int)((expected >> (int)i) & 1))
                {
                    return false;
                }
            }
        }
        return true;
    }

    // ---- Banding (StandardBanding) -------------------------------------------------------------

    /// <summary>
    /// Port of StandardBanding for Standard128Ribbon (no backtracking: ResetAndFindSeedToSolve resets
    /// the whole banding for each seed). Arrays are rented from <see cref="ArrayPool{T}"/>.
    /// </summary>
    public sealed class Banding : IDisposable
    {
        private ulong[] _coeffLo = [];
        private ulong[] _coeffHi = [];
        private uint[] _result = [];
        private uint _numStarts;

        public uint NumStarts => _numStarts;

        public uint OrdinalSeed { get; private set; }

        private void Reset(uint numSlots)
        {
            if (_coeffLo.Length < numSlots)
            {
                ReturnArrays();
                _coeffLo = ArrayPool<ulong>.Shared.Rent((int)numSlots);
                _coeffHi = ArrayPool<ulong>.Shared.Rent((int)numSlots);
                _result = ArrayPool<uint>.Shared.Rent((int)numSlots);
            }
            Array.Clear(_coeffLo, 0, (int)numSlots);
            Array.Clear(_coeffHi, 0, (int)numSlots);
            Array.Clear(_result, 0, (int)numSlots);
            _numStarts = numSlots - CoeffBits + 1;
        }

        /// <summary>BandingAdd with kFirstCoeffAlwaysOne (ribbon_alg.h).</summary>
        private bool BandingAdd(uint start, uint rr, ulong crLo, ulong crHi)
        {
            ulong[] coeffLo = _coeffLo, coeffHi = _coeffHi;
            uint[] result = _result;
            uint i = start;
            for (; ; )
            {
                ulong atLo = coeffLo[i], atHi = coeffHi[i];
                if ((atLo | atHi) == 0)
                {
                    coeffLo[i] = crLo;
                    coeffHi[i] = crHi;
                    result[i] = rr;
                    return true;
                }
                crLo ^= atLo;
                crHi ^= atHi;
                rr ^= result[i];
                int tz;
                if (crLo != 0)
                {
                    tz = BitOperations.TrailingZeroCount(crLo);
                }
                else if (crHi != 0)
                {
                    tz = 64 + BitOperations.TrailingZeroCount(crHi);
                }
                else
                {
                    break;
                }
                i += (uint)tz;
                ShiftRight(crLo, crHi, tz, out crLo, out crHi);
            }
            // Linearly dependent: OK only if result rows also cancel
            return rr == 0;
        }

        private bool AddRange(ReadOnlySpan<ulong> keyHashes, uint rawSeed)
        {
            uint numStarts = _numStarts;
            foreach (ulong keyHash in keyHashes)
            {
                ulong h = Rehash(keyHash, rawSeed);
                uint start = GetStart(h, numStarts);
                uint rr = GetResultRowFromHash(h);
                GetCoeffRow(h, out ulong crLo, out ulong crHi);
                if (!BandingAdd(start, rr, crLo, crHi))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>StandardBanding::ResetAndFindSeedToSolve.</summary>
        public bool ResetAndFindSeedToSolve(uint numSlots, ReadOnlySpan<ulong> keyHashes, uint startingOrdinalSeed, uint ordinalSeedMask)
        {
            startingOrdinalSeed &= ordinalSeedMask;
            uint cur = startingOrdinalSeed;
            do
            {
                OrdinalSeed = cur;
                Reset(numSlots);
                if (AddRange(keyHashes, OrdinalToRawSeed(cur)))
                {
                    return true;
                }
                cur = (cur + 1) & ordinalSeedMask;
            } while (cur != startingOrdinalSeed);
            return false;
        }

        /// <summary>
        /// InterleavedBackSubst: solves the banding into the serialized interleaved solution in
        /// <paramref name="data"/> (length = segments * 16, excluding metadata).
        /// </summary>
        public void BackSubstInto(Span<byte> data, in Layout layout)
        {
            uint numBlocks = layout.NumBlocks;
            uint numSegments = layout.NumSegments;
            uint numColumns = layout.UpperNumColumns;
            uint upperStartBlock = layout.UpperStartBlock;
            if (numColumns == 0)
            {
                return;
            }
            Span<ulong> stateLo = stackalloc ulong[ResultBits];
            Span<ulong> stateHi = stackalloc ulong[ResultBits];
            stateLo.Clear();
            stateHi.Clear();

            uint block = numBlocks;
            uint segmentNum = numSegments;
            while (block > upperStartBlock)
            {
                --block;
                BackSubstBlock(stateLo, stateHi, numColumns, block * CoeffBits);
                segmentNum -= numColumns;
                StoreSegments(data, segmentNum, numColumns, stateLo, stateHi);
            }
            // Now (if applicable), region using lower number of columns
            --numColumns;
            while (block > 0)
            {
                --block;
                BackSubstBlock(stateLo, stateHi, numColumns, block * CoeffBits);
                segmentNum -= numColumns;
                StoreSegments(data, segmentNum, numColumns, stateLo, stateHi);
            }
        }

        private static void StoreSegments(Span<byte> data, uint segmentNum, uint numColumns, Span<ulong> stateLo, Span<ulong> stateHi)
        {
            for (int i = 0; i < numColumns; ++i)
            {
                Span<byte> s = data.Slice((int)((segmentNum + (uint)i) * BytesPerSegment), BytesPerSegment);
                BinaryPrimitives.WriteUInt64LittleEndian(s, stateLo[i]);
                BinaryPrimitives.WriteUInt64LittleEndian(s[8..], stateHi[i]);
            }
        }

        private void BackSubstBlock(Span<ulong> stateLo, Span<ulong> stateHi, uint numColumns, uint startSlot)
        {
            for (uint i = startSlot + CoeffBits; i > startSlot;)
            {
                --i;
                ulong crLo = _coeffLo[i], crHi = _coeffHi[i];
                uint rr = _result[i];
                for (int j = 0; j < numColumns; ++j)
                {
                    // Compute next solution bit at row i, column j (see derivation in ribbon_alg.h)
                    ulong tLo = stateLo[j] << 1;
                    ulong tHi = (stateHi[j] << 1) | (stateLo[j] >> 63);
                    ulong bit = (ulong)(Parity(tLo & crLo, tHi & crHi) ^ (int)((rr >> j) & 1));
                    stateLo[j] = tLo | bit;
                    stateHi[j] = tHi;
                }
            }
        }

        private void ReturnArrays()
        {
            if (_coeffLo.Length > 0)
            {
                ArrayPool<ulong>.Shared.Return(_coeffLo);
                ArrayPool<ulong>.Shared.Return(_coeffHi);
                ArrayPool<uint>.Shared.Return(_result);
            }
            _coeffLo = [];
            _coeffHi = [];
            _result = [];
        }

        public void Dispose() => ReturnArrays();
    }
}
