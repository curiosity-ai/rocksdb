using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Compression;

/// <summary>
/// LZ4 block-format encoder for inputs below 64 KiB (all usual RocksDB data and index blocks). Port of
/// LZ4_compress_generic (lz4.c 1.9/1.10) for <c>tableType = byU16</c>, <c>noDict</c>, <c>limitedOutput</c>,
/// acceleration 1 - the code path LZ4_compress_fast takes for such inputs, so the output is the same as native liblz4's.
/// </summary>
/// <remarks>
/// Replaces K4os' LZ4Codec.Encode for these sizes: K4os allocates and zeroes a 16 KiB LZ4_stream_t per call
/// (unmanaged memory) and its generic engine runs 2.2-3x slower than native liblz4 for 1-16 KiB blocks (docs/perf.md);
/// this encoder uses a per-thread hash table and specialized loops. Larger inputs and LZ4HC still use K4os.
/// The hash table holds 16-bit positions (<see cref="HashTableSize"/> entries) and is cleared per call, like
/// LZ4_initStream.
/// </remarks>
[SkipLocalsInit]
internal static unsafe class Lz4BlockEncoder
{
    private const int MinMatch = 4;
    private const int LastLiterals = 5;
    private const int MfLimit = 12;
    private const int MinLength = MfLimit + 1;
    private const int MlBits = 4;
    private const uint MlMask = (1U << MlBits) - 1;
    private const int RunBits = 8 - MlBits;
    private const uint RunMask = (1U << RunBits) - 1;
    private const int SkipTrigger = 6;
    private const int HashLog = 12; // LZ4_MEMORY_USAGE 14
    private const int HashTableSize = 1 << (HashLog + 1); // byU16 uses one more hash bit
    /// <summary>LZ4_64Klimit: inputs shorter than this use 16-bit positions (byU16).</summary>
    public const int Limit64K = 64 * 1024 + (MfLimit - 1);

    [ThreadStatic] private static ushort[]? t_table;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Hash4(uint sequence) => (sequence * 2654435761U) >> (MinMatch * 8 - (HashLog + 1));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Read32(byte* p) => Unsafe.ReadUnaligned<uint>(p);

    /// <summary>
    /// Compresses <paramref name="source"/> (shorter than <see cref="Limit64K"/>) into <paramref name="destination"/>.
    /// Returns the compressed size, or 0 when it does not fit (LZ4_compress_fast's limitedOutput contract).
    /// </summary>
    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if ((uint)source.Length >= Limit64K) ThrowHelper.InvalidArgument("LZ4 input too large for the 16-bit encoder");
        ushort[] table = t_table ??= new ushort[HashTableSize];
        table.AsSpan().Clear();
        fixed (byte* src = source)
        fixed (byte* dst = destination)
        fixed (ushort* t = table)
        {
            return CompressCore(src, dst, source.Length, destination.Length, t);
        }
    }

    /// <summary>
    /// Port of LZ4_compress_generic_validated (byU16, noDict, limitedOutput, acceleration 1). AggressiveOptimization:
    /// compiled fully optimized on first use; the tier-0 / OSR versions of this loop made the first flushes of a process
    /// 3-4x slower (docs/perf.md).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static int CompressCore(byte* source, byte* dest, int inputSize, int maxOutputSize, ushort* table)
    {
        byte* ip = source;
        byte* @base = source;
        byte* lowLimit = source;
        byte* anchor = source;
        byte* iend = ip + inputSize;
        byte* mflimitPlusOne = iend - MfLimit + 1;
        byte* matchlimit = iend - LastLiterals;
        byte* op = dest;
        byte* olimit = op + maxOutputSize;
        byte* match;
        byte* token;
        uint forwardH;

        if (inputSize < MinLength) goto LastLiteralsLabel; // Input too small, no compression (all literals)

        // First Byte
        table[Hash4(Read32(ip))] = 0;
        ip++;
        forwardH = Hash4(Read32(ip));

        // Main Loop
        while (true)
        {
            // Find a match
            {
                byte* forwardIp = ip;
                int step = 1;
                int searchMatchNb = 1 << SkipTrigger;
                while (true)
                {
                    uint h = forwardH;
                    uint current = (uint)(forwardIp - @base);
                    uint matchIndex = table[h];
                    ip = forwardIp;
                    forwardIp += step;
                    step = searchMatchNb++ >> SkipTrigger;
                    if (forwardIp > mflimitPlusOne) goto LastLiteralsLabel;
                    match = @base + matchIndex;
                    forwardH = Hash4(Read32(forwardIp));
                    table[h] = (ushort)current;
                    if (Read32(match) == Read32(ip)) break;
                }
            }

            // Catch up
            while (ip > anchor && match > lowLimit && ip[-1] == match[-1])
            {
                ip--;
                match--;
            }

            // Encode Literals
            {
                uint litLength = (uint)(ip - anchor);
                token = op++;
                if (op + litLength + (2 + 1 + LastLiterals) + litLength / 255 > olimit) return 0; // Check output buffer overflow
                if (litLength >= RunMask)
                {
                    uint len = litLength - RunMask;
                    *token = (byte)(RunMask << MlBits);
                    for (; len >= 255; len -= 255) *op++ = 255;
                    *op++ = (byte)len;
                }
                else
                {
                    *token = (byte)(litLength << MlBits);
                }
                // Copy Literals
                Unsafe.CopyBlockUnaligned(op, anchor, litLength);
                op += litLength;
            }

        NextMatch:
            // at this stage, the following variables must be correctly set :
            // - ip : at start of LZ operation
            // - match : at start of previous pattern occurrence; can be within current prefix, or within extDict
            // - token and *token : position to write 4-bits for match length; higher 4-bits for literal length supposed already written

            // Encode Offset
            BinaryPrimitives.WriteUInt16LittleEndian(new Span<byte>(op, 2), (ushort)(ip - match));
            op += 2;

            // Encode MatchLength
            {
                uint matchCode = Count(ip + MinMatch, match + MinMatch, matchlimit);
                ip += matchCode + MinMatch;
                if (op + (1 + LastLiterals) + (matchCode + 240) / 255 > olimit) return 0; // Check output buffer overflow
                if (matchCode >= MlMask)
                {
                    *token += (byte)MlMask;
                    matchCode -= MlMask;
                    Unsafe.WriteUnaligned(op, 0xFFFFFFFFu);
                    while (matchCode >= 4 * 255)
                    {
                        op += 4;
                        Unsafe.WriteUnaligned(op, 0xFFFFFFFFu);
                        matchCode -= 4 * 255;
                    }
                    op += matchCode / 255;
                    *op++ = (byte)(matchCode % 255);
                }
                else
                {
                    *token += (byte)matchCode;
                }
            }

            anchor = ip;

            // Test end of chunk
            if (ip >= mflimitPlusOne) break;

            // Fill table
            table[Hash4(Read32(ip - 2))] = (ushort)(ip - 2 - @base);

            // Test next position
            {
                uint h = Hash4(Read32(ip));
                uint current = (uint)(ip - @base);
                uint matchIndex = table[h];
                match = @base + matchIndex;
                table[h] = (ushort)current;
                if (Read32(match) == Read32(ip))
                {
                    token = op++;
                    *token = 0;
                    goto NextMatch;
                }
            }

            // Prepare next loop
            forwardH = Hash4(Read32(++ip));
        }

    LastLiteralsLabel:
        // Encode Last Literals
        {
            ulong lastRun = (ulong)(iend - anchor);
            if ((ulong)(op - dest) + lastRun + 1 + (lastRun + 255 - RunMask) / 255 > (ulong)maxOutputSize) return 0; // adapt lastRun to fill 'dst'
            if (lastRun >= RunMask)
            {
                ulong accumulator = lastRun - RunMask;
                *op++ = (byte)(RunMask << MlBits);
                for (; accumulator >= 255; accumulator -= 255) *op++ = 255;
                *op++ = (byte)accumulator;
            }
            else
            {
                *op++ = (byte)(lastRun << MlBits);
            }
            Unsafe.CopyBlockUnaligned(op, anchor, (uint)lastRun);
            op += lastRun;
        }
        return (int)(op - dest);
    }

    /// <summary>Port of LZ4_count: length of the common prefix of pIn and pMatch, not reading at or beyond pInLimit.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Count(byte* pIn, byte* pMatch, byte* pInLimit)
    {
        byte* pStart = pIn;
        if (pIn < pInLimit - 7)
        {
            ulong diff = Unsafe.ReadUnaligned<ulong>(pMatch) ^ Unsafe.ReadUnaligned<ulong>(pIn);
            if (diff != 0) return (uint)BitOperations.TrailingZeroCount(diff) >> 3;
            pIn += 8;
            pMatch += 8;
        }
        while (pIn < pInLimit - 7)
        {
            ulong diff = Unsafe.ReadUnaligned<ulong>(pMatch) ^ Unsafe.ReadUnaligned<ulong>(pIn);
            if (diff == 0)
            {
                pIn += 8;
                pMatch += 8;
                continue;
            }
            pIn += BitOperations.TrailingZeroCount(diff) >> 3;
            return (uint)(pIn - pStart);
        }
        if (pIn < pInLimit - 3 && Read32(pMatch) == Read32(pIn))
        {
            pIn += 4;
            pMatch += 4;
        }
        if (pIn < pInLimit - 1 && Unsafe.ReadUnaligned<ushort>(pMatch) == Unsafe.ReadUnaligned<ushort>(pIn))
        {
            pIn += 2;
            pMatch += 2;
        }
        if (pIn < pInLimit && *pMatch == *pIn) pIn++;
        return (uint)(pIn - pStart);
    }
}
