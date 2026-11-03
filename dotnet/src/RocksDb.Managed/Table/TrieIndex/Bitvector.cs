using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table.TrieIndex;

/// <summary>
/// Bit helpers and constants of utilities/trie_index/bitvector.h (kBitsPerRankSample, kOnesPerSelectHint,
/// FindNthSetBitInWord) and the little-endian array readers used by the trie's deserialization.
/// </summary>
internal static class TrieBits
{
    /// <summary>kBitsPerRankSample: bits per rank lookup table entry.</summary>
    public const ulong BitsPerRankSample = 256;
    /// <summary>kWordsPerRankSample.</summary>
    public const ulong WordsPerRankSample = BitsPerRankSample / 64;
    /// <summary>kOnesPerSelectHint: ones (or zeros) between select hint entries.</summary>
    public const ulong OnesPerSelectHint = 256;

    /// <summary>
    /// FindNthSetBitInWord: position of the <paramref name="i"/>-th set bit (0-indexed) of <paramref name="word"/>
    /// (precondition: i &lt; popcount(word)). BMI2 PDEP when available, else the popcount binary search of the C++
    /// fallback (SuRF's select64_popcount_search).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong FindNthSetBitInWord(ulong word, ulong i)
    {
        if (Bmi2.X64.IsSupported) return (ulong)BitOperations.TrailingZeroCount(Bmi2.X64.ParallelBitDeposit(1UL << (int)i, word));
        return FindNthSetBitInWordPortable(word, i);
    }

    internal static ulong FindNthSetBitInWordPortable(ulong word, ulong i)
    {
        ulong pos = 0;
        ulong pc = (ulong)BitOperations.PopCount(word & 0xFFFFFFFFUL);
        if (i >= pc) { word >>= 32; pos += 32; i -= pc; }
        pc = (ulong)BitOperations.PopCount(word & 0xFFFFUL);
        if (i >= pc) { word >>= 16; pos += 16; i -= pc; }
        pc = (ulong)BitOperations.PopCount(word & 0xFFUL);
        if (i >= pc) { word >>= 8; pos += 8; i -= pc; }
        pc = (ulong)BitOperations.PopCount(word & 0xFUL);
        if (i >= pc) { word >>= 4; pos += 4; i -= pc; }
        pc = (ulong)BitOperations.PopCount(word & 0x3UL);
        if (i >= pc) { word >>= 2; pos += 2; i -= pc; }
        pc = word & 1;
        if (i >= pc) pos += 1;
        return pos;
    }

    /// <summary>(bytes + 7) &amp; ~7.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Pad8(ulong bytes) => (bytes + 7) & ~7UL;

    /// <summary>Appends zero bytes up to the next multiple of 8 of <paramref name="bytesWritten"/>.</summary>
    public static void WritePadding(IBufferWriter<byte> w, ulong bytesWritten)
    {
        int padding = (int)((8 - (bytesWritten % 8)) % 8);
        if (padding == 0) return;
        w.GetSpan(padding)[..padding].Clear();
        w.Advance(padding);
    }

    /// <summary>Reads <paramref name="count"/> little-endian 64-bit values.</summary>
    public static ulong[] ReadUInt64s(ReadOnlySpan<byte> src, int count)
    {
        if (count == 0) return [];
        ulong[] dst = GC.AllocateUninitializedArray<ulong>(count);
        ReadOnlySpan<ulong> raw = MemoryMarshal.Cast<byte, ulong>(src[..(count * 8)]);
        if (BitConverter.IsLittleEndian) raw.CopyTo(dst);
        else BinaryPrimitives.ReverseEndianness(raw, dst);
        return dst;
    }

    /// <summary>Reads <paramref name="count"/> little-endian 32-bit values.</summary>
    public static uint[] ReadUInt32s(ReadOnlySpan<byte> src, int count)
    {
        if (count == 0) return [];
        uint[] dst = GC.AllocateUninitializedArray<uint>(count);
        ReadOnlySpan<uint> raw = MemoryMarshal.Cast<byte, uint>(src[..(count * 4)]);
        if (BitConverter.IsLittleEndian) raw.CopyTo(dst);
        else BinaryPrimitives.ReverseEndianness(raw, dst);
        return dst;
    }

    /// <summary>Reads <paramref name="count"/> little-endian 16-bit values.</summary>
    public static ushort[] ReadUInt16s(ReadOnlySpan<byte> src, int count)
    {
        if (count == 0) return [];
        ushort[] dst = GC.AllocateUninitializedArray<ushort>(count);
        ReadOnlySpan<ushort> raw = MemoryMarshal.Cast<byte, ushort>(src[..(count * 2)]);
        if (BitConverter.IsLittleEndian) raw.CopyTo(dst);
        else BinaryPrimitives.ReverseEndianness(raw, dst);
        return dst;
    }

    /// <summary>Appends the values as little-endian 32-bit words followed by 8-byte alignment padding.</summary>
    public static void WriteUInt32sPadded(IBufferWriter<byte> w, ReadOnlySpan<uint> values)
    {
        foreach (uint v in values) Coding.PutFixed32(w, v);
        WritePadding(w, (ulong)values.Length * 4);
    }
}

/// <summary>
/// Port of BitvectorBuilder (utilities/trie_index/bitvector.h): builds a bitvector bit by bit (LSB-first within
/// 64-bit words).
/// </summary>
internal sealed class BitvectorBuilder
{
    private ulong[] _words = [];
    private int _numWords;
    private ulong _numBits;

    public ulong NumBits => _numBits;

    /// <summary>The written words.</summary>
    public ReadOnlySpan<ulong> Words => _words.AsSpan(0, _numWords);

    private void PushWord(ulong w)
    {
        if (_numWords == _words.Length) Array.Resize(ref _words, Math.Max(4, _words.Length * 2));
        _words[_numWords++] = w;
    }

    /// <summary>Append: a single bit.</summary>
    public void Append(bool bit)
    {
        if (_numBits % 64 == 0) PushWord(0);
        if (bit) _words[_numWords - 1] |= 1UL << (int)(_numBits % 64);
        _numBits++;
    }

    /// <summary>AppendWord: <paramref name="nbits"/> (1..64) bits from the low bits of <paramref name="word"/>; requires word alignment.</summary>
    public void AppendWord(ulong word, ulong nbits)
    {
        System.Diagnostics.Debug.Assert(nbits is > 0 and <= 64 && _numBits % 64 == 0);
        PushWord(word);
        _numBits += nbits;
    }

    /// <summary>AppendMultiple: <paramref name="count"/> copies of <paramref name="bit"/>.</summary>
    public void AppendMultiple(bool bit, ulong count)
    {
        if (count == 0) return;
        ulong partial = _numBits % 64;
        if (partial > 0)
        {
            ulong fill = Math.Min(count, 64 - partial);
            if (bit)
            {
                ulong mask = (fill == 64 ? ~0UL : (1UL << (int)fill) - 1) << (int)partial;
                _words[_numWords - 1] |= mask;
            }
            _numBits += fill;
            count -= fill;
        }
        ulong full = bit ? ~0UL : 0UL;
        while (count >= 64)
        {
            PushWord(full);
            _numBits += 64;
            count -= 64;
        }
        if (count > 0)
        {
            PushWord(bit ? (1UL << (int)count) - 1 : 0);
            _numBits += count;
        }
    }

    /// <summary>Reserve: capacity for <paramref name="numBits"/> bits.</summary>
    public void Reserve(ulong numBits)
    {
        int words = (int)((numBits + 63) / 64);
        if (words > _words.Length) Array.Resize(ref _words, words);
    }

    /// <summary>GetBit (testing).</summary>
    public bool GetBit(ulong pos) => ((_words[(int)(pos / 64)] >> (int)(pos % 64)) & 1) != 0;
}

/// <summary>
/// Port of Bitvector (utilities/trie_index/bitvector.{h,cc}): an immutable bitvector with O(1) rank (a uint32 rank
/// LUT every 256 bits) and select (select hints every 256 ones / zeros). Serialized as
/// <c>[num_bits u64][num_ones u64][words][rank LUT u32, padded to 8][select1 hints u32, padded][select0 hints u32, padded]</c>.
/// Deserialization copies the arrays into typed (endian-neutral) arrays instead of aliasing the block bytes.
/// </summary>
internal sealed class Bitvector
{
    private ulong[] _words = [];
    private uint[] _rankLut = [];
    private uint[] _select1 = [];
    private uint[] _select0 = [];
    private ulong _numBits;
    private ulong _numOnes;
    private ulong _numWords;
    private ulong _numRankSamples;
    private ulong _numSelect1;
    private ulong _numSelect0;

    public ulong NumBits => _numBits;
    public ulong NumOnes => _numOnes;
    public ulong NumZeros => _numBits - _numOnes;

    /// <summary>Approximate heap bytes of the deserialized arrays.</summary>
    public ulong MemoryUsage => (ulong)_words.Length * 8 + ((ulong)_rankLut.Length + (ulong)_select1.Length + (ulong)_select0.Length) * 4;

    /// <summary>BuildFrom: builds the rank LUT and select hints over the builder's bits.</summary>
    public static Bitvector BuildFrom(BitvectorBuilder builder)
    {
        var bv = new Bitvector
        {
            _numBits = builder.NumBits,
        };
        bv._numWords = (bv._numBits + 63) / 64;
        bv._numRankSamples = bv._numBits / TrieBits.BitsPerRankSample + 1;
        bv._words = builder.Words[..(int)bv._numWords].ToArray();
        bv._rankLut = new uint[bv._numRankSamples];
        bv.BuildRankLut();
        bv._numSelect1 = bv._numOnes > 0 ? (bv._numOnes - 1) / TrieBits.OnesPerSelectHint + 1 : 0;
        ulong zeros = bv._numBits - bv._numOnes;
        bv._numSelect0 = zeros > 0 ? (zeros - 1) / TrieBits.OnesPerSelectHint + 1 : 0;
        bv._select1 = new uint[bv._numSelect1];
        bv._select0 = new uint[bv._numSelect0];
        bv.BuildSelectHints();
        return bv;
    }

    private void BuildRankLut()
    {
        System.Diagnostics.Debug.Assert(_numBits <= uint.MaxValue);
        ulong cumulative = 0;
        for (ulong s = 0; s < _numRankSamples; s++)
        {
            _rankLut[s] = (uint)cumulative;
            ulong ws = s * TrieBits.WordsPerRankSample;
            ulong we = Math.Min(ws + TrieBits.WordsPerRankSample, _numWords);
            for (ulong w = ws; w < we; w++) cumulative += (ulong)BitOperations.PopCount(_words[w]);
        }
        _numOnes = cumulative;
    }

    private void BuildSelectHints()
    {
        ulong sample = 0;
        for (ulong k = 0; k < _numSelect1; k++)
        {
            ulong target = k * TrieBits.OnesPerSelectHint;
            while (sample + 1 < _numRankSamples && _rankLut[sample + 1] <= target) sample++;
            _select1[k] = (uint)sample;
        }
        sample = 0;
        for (ulong k = 0; k < _numSelect0; k++)
        {
            ulong target = k * TrieBits.OnesPerSelectHint;
            while (sample + 1 < _numRankSamples && (sample + 1) * TrieBits.BitsPerRankSample - _rankLut[sample + 1] <= target) sample++;
            _select0[k] = (uint)sample;
        }
    }

    /// <summary>SerializedSize: bytes of <see cref="EncodeTo"/>.</summary>
    public ulong SerializedSize =>
        16 + _numWords * 8 + TrieBits.Pad8(_numRankSamples * 4) + TrieBits.Pad8(_numSelect1 * 4) + TrieBits.Pad8(_numSelect0 * 4);

    /// <summary>EncodeTo: appends the serialized form.</summary>
    public void EncodeTo(IBufferWriter<byte> w)
    {
        Coding.PutFixed64(w, _numBits);
        Coding.PutFixed64(w, _numOnes);
        for (ulong i = 0; i < _numWords; i++) Coding.PutFixed64(w, _words[i]);
        if (_numRankSamples > 0) TrieBits.WriteUInt32sPadded(w, _rankLut);
        if (_numSelect1 > 0) TrieBits.WriteUInt32sPadded(w, _select1);
        if (_numSelect0 > 0) TrieBits.WriteUInt32sPadded(w, _select0);
    }

    /// <summary>
    /// InitFromData: parses a serialized bitvector at <paramref name="pos"/> of <paramref name="data"/> and advances
    /// <paramref name="pos"/>. Alignment is checked relative to the start of <paramref name="data"/> (the C++ code checks
    /// the absolute address of a buffer it re-aligns first). Throws Corruption with upstream's messages.
    /// </summary>
    public static Bitvector Read(ReadOnlySpan<byte> data, ref int pos)
    {
        var bv = new Bitvector();
        ulong remaining = (ulong)(data.Length - pos);
        if (remaining < 16) ThrowHelper.Corruption("Bitvector: insufficient data for header");
        bv._numBits = Coding.DecodeFixed64(data[pos..]);
        bv._numOnes = Coding.DecodeFixed64(data[(pos + 8)..]);
        int p = pos + 16;
        remaining -= 16;
        if (bv._numOnes > bv._numBits) ThrowHelper.Corruption("Bitvector: num_ones exceeds num_bits");
        if (bv._numBits > uint.MaxValue) ThrowHelper.Corruption("Bitvector: num_bits exceeds uint32_t range for rank LUT");
        bv._numWords = (bv._numBits + 63) / 64;
        bv._numRankSamples = bv._numBits / TrieBits.BitsPerRankSample + 1;
        bv._numSelect1 = bv._numOnes > 0 ? (bv._numOnes - 1) / TrieBits.OnesPerSelectHint + 1 : 0;
        ulong zeros = bv._numBits - bv._numOnes;
        bv._numSelect0 = zeros > 0 ? (zeros - 1) / TrieBits.OnesPerSelectHint + 1 : 0;

        ulong wordsBytes = bv._numWords * 8;
        if (remaining < wordsBytes) ThrowHelper.Corruption("Bitvector: insufficient data for words");
        if (p % 8 != 0) ThrowHelper.Corruption("Bitvector: words data not 8-byte aligned");
        bv._words = TrieBits.ReadUInt64s(data[p..], (int)bv._numWords);
        p += (int)wordsBytes;
        remaining -= wordsBytes;

        bv._rankLut = ReadU32Array(data, ref p, ref remaining, bv._numRankSamples, "rank LUT");
        bv._select1 = ReadU32Array(data, ref p, ref remaining, bv._numSelect1, "select1 hints");
        bv._select0 = ReadU32Array(data, ref p, ref remaining, bv._numSelect0, "select0 hints");
        for (ulong i = 0; i < bv._numSelect1; i++)
            if (bv._select1[i] >= bv._numRankSamples) ThrowHelper.Corruption("Bitvector: select1 hint value exceeds rank LUT size");
        for (ulong i = 0; i < bv._numSelect0; i++)
            if (bv._select0[i] >= bv._numRankSamples) ThrowHelper.Corruption("Bitvector: select0 hint value exceeds rank LUT size");
        pos = p;
        return bv;
    }

    private static uint[] ReadU32Array(ReadOnlySpan<byte> data, ref int p, ref ulong remaining, ulong count, string name)
    {
        if (count == 0) return [];
        ulong padded = TrieBits.Pad8(count * 4);
        if (remaining < padded) ThrowHelper.Corruption("Bitvector: insufficient data for " + name);
        if (p % 4 != 0) ThrowHelper.Corruption("Bitvector: " + name + " not 4-byte aligned");
        uint[] arr = TrieBits.ReadUInt32s(data[p..], (int)count);
        p += (int)padded;
        remaining -= padded;
        return arr;
    }

    // ---- Core operations ----

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool GetBit(ulong pos) => ((_words[(int)(pos >> 6)] >> (int)(pos & 63)) & 1) != 0;

    /// <summary>rank1(pos): ones in [0, pos); pos in [0, NumBits].</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong Rank1(ulong pos)
    {
        ulong sampleIdx = pos / TrieBits.BitsPerRankSample;
        ulong rank = _rankLut[(int)sampleIdx];
        ulong[] words = _words;
        int wordStart = (int)(sampleIdx * TrieBits.WordsPerRankSample);
        int wordEnd = (int)(pos >> 6);
        switch (wordEnd - wordStart)
        {
            case 3:
                rank += (ulong)BitOperations.PopCount(words[wordStart + 2]);
                goto case 2;
            case 2:
                rank += (ulong)BitOperations.PopCount(words[wordStart + 1]);
                goto case 1;
            case 1:
                rank += (ulong)BitOperations.PopCount(words[wordStart]);
                break;
        }
        int rem = (int)(pos & 63);
        if (rem > 0) rank += (ulong)BitOperations.PopCount(words[wordEnd] & ((1UL << rem) - 1));
        return rank;
    }

    /// <summary>rank0(pos): zeros in [0, pos).</summary>
    public ulong Rank0(ulong pos) => pos - Rank1(pos);

    /// <summary>select1(i): position of the i-th one (0-indexed), NumBits when there are fewer ones.</summary>
    public ulong FindNthOneBit(ulong i)
    {
        if (i >= _numOnes) return _numBits;
        ulong lo, hi;
        if (_numSelect1 > 0)
        {
            ulong hint = i / TrieBits.OnesPerSelectHint;
            lo = _select1[(int)hint];
            hi = hint + 1 < _numSelect1 ? _select1[(int)hint + 1] : _numRankSamples - 1;
        }
        else
        {
            lo = 0;
            hi = _numRankSamples - 1;
        }
        while (lo < hi && _rankLut[(int)lo + 1] <= i) lo++;
        ulong remaining = i - _rankLut[(int)lo];
        ulong ws = lo * TrieBits.WordsPerRankSample;
        ulong we = Math.Min(ws + TrieBits.WordsPerRankSample, _numWords);
        for (ulong w = ws; w < we; w++)
        {
            ulong pc = (ulong)BitOperations.PopCount(_words[w]);
            if (remaining < pc) return w * 64 + TrieBits.FindNthSetBitInWord(_words[w], remaining);
            remaining -= pc;
        }
        return _numBits;
    }

    /// <summary>select0(i): position of the i-th zero (0-indexed), NumBits when there are fewer zeros.</summary>
    public ulong FindNthZeroBit(ulong i)
    {
        ulong zeros = _numBits - _numOnes;
        if (i >= zeros) return _numBits;
        ulong lo, hi;
        if (_numSelect0 > 0)
        {
            ulong hint = i / TrieBits.OnesPerSelectHint;
            lo = _select0[(int)hint];
            hi = hint + 1 < _numSelect0 ? _select0[(int)hint + 1] : _numRankSamples - 1;
        }
        else
        {
            lo = 0;
            hi = _numRankSamples - 1;
        }
        while (lo < hi)
        {
            ulong zerosNext = (lo + 1) * TrieBits.BitsPerRankSample - _rankLut[(int)lo + 1];
            if (zerosNext <= i) lo++;
            else break;
        }
        ulong zerosAtLo = lo * TrieBits.BitsPerRankSample - _rankLut[(int)lo];
        ulong remaining = i - zerosAtLo;
        ulong ws = lo * TrieBits.WordsPerRankSample;
        ulong we = Math.Min(ws + TrieBits.WordsPerRankSample, _numWords);
        for (ulong w = ws; w < we; w++)
        {
            ulong valid = 64;
            if (w == _numWords - 1 && _numBits % 64 != 0) valid = _numBits % 64;
            ulong zerosInWord = valid - (ulong)BitOperations.PopCount(_words[w]);
            if (remaining < zerosInWord)
            {
                ulong word = ~_words[w];
                if (valid < 64) word &= (1UL << (int)valid) - 1;
                return w * 64 + TrieBits.FindNthSetBitInWord(word, remaining);
            }
            remaining -= zerosInWord;
        }
        return _numBits;
    }

    /// <summary>NextSetBit: the first set bit at or after <paramref name="pos"/>, NumBits when none.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong NextSetBit(ulong pos)
    {
        if (pos >= _numBits) return _numBits;
        int wi = (int)(pos >> 6);
        ulong word = _words[wi] >> (int)(pos & 63);
        if (word != 0)
        {
            ulong r = pos + (ulong)BitOperations.TrailingZeroCount(word);
            return r < _numBits ? r : _numBits;
        }
        return NextSetBitSlow(wi + 1);
    }

    private ulong NextSetBitSlow(int fromWord)
    {
        ulong[] words = _words;
        for (int w = fromWord; w < (int)_numWords; w++)
        {
            if (words[w] != 0)
            {
                ulong r = (ulong)w * 64 + (ulong)BitOperations.TrailingZeroCount(words[w]);
                return r < _numBits ? r : _numBits;
            }
        }
        return _numBits;
    }

    /// <summary>PrevSetBit: the last set bit strictly before <paramref name="pos"/>, NumBits (sentinel) when none.</summary>
    public ulong PrevSetBit(ulong pos)
    {
        if (pos == 0 || _numBits == 0) return _numBits;
        if (pos > _numBits) pos = _numBits;
        pos--;
        int wi = (int)(pos >> 6);
        int bi = (int)(pos & 63);
        ulong mask = bi == 63 ? ~0UL : (1UL << (bi + 1)) - 1;
        ulong word = _words[wi] & mask;
        if (word != 0) return (ulong)wi * 64 + (ulong)BitOperations.Log2(word);
        for (int w = wi - 1; w >= 0; w--)
            if (_words[w] != 0) return (ulong)w * 64 + (ulong)BitOperations.Log2(_words[w]);
        return _numBits;
    }

    /// <summary>DistanceToNextSetBit: distance from the set bit at <paramref name="pos"/> to the next set bit.</summary>
    public ulong DistanceToNextSetBit(ulong pos) => NextSetBit(pos + 1) - pos;
}

/// <summary>
/// Port of EliasFano (utilities/trie_index/bitvector.{h,cc}): a monotone sequence as unary-coded high parts (a
/// <see cref="Bitvector"/>) plus packed low parts. Serialized as <c>[count u64][universe u64][low_bits u64][high
/// bitvector][low words]</c>. Not used by the trie format itself (kept with the bitvector like upstream).
/// </summary>
internal sealed class EliasFano
{
    private ulong _count;
    private ulong _universe;
    private ulong _lowBits;
    private ulong _lowMask;
    private Bitvector _high = Bitvector.BuildFrom(new BitvectorBuilder());
    private ulong[] _lowWords = [];

    public ulong Count => _count;
    public ulong Universe => _universe;

    /// <summary>BuildFrom: <paramref name="values"/> must be non-decreasing.</summary>
    public static EliasFano BuildFrom(ReadOnlySpan<ulong> values, ulong universe)
    {
        var ef = new EliasFano { _count = (ulong)values.Length, _universe = universe };
        ulong count = ef._count;
        if (count == 0) return ef;
        ef._lowBits = universe > count ? (ulong)BitOperations.Log2(universe / count) : 0;
        ef._lowMask = ef._lowBits < 64 ? (1UL << (int)ef._lowBits) - 1 : ~0UL;
        ulong maxHigh = ef._lowBits < 64 ? values[^1] >> (int)ef._lowBits : 0;
        var hb = new BitvectorBuilder();
        hb.Reserve(maxHigh + count + 1);
        ulong prevHigh = 0;
        foreach (ulong v in values)
        {
            ulong high = ef._lowBits < 64 ? v >> (int)ef._lowBits : 0;
            hb.AppendMultiple(false, high - prevHigh);
            hb.Append(true);
            prevHigh = high;
        }
        ef._high = Bitvector.BuildFrom(hb);
        ulong numLowWords = (count * ef._lowBits + 63) / 64;
        ef._lowWords = new ulong[numLowWords];
        for (ulong i = 0; i < count; i++)
        {
            ulong low = values[(int)i] & ef._lowMask;
            ulong bitPos = i * ef._lowBits;
            int wi = (int)(bitPos / 64);
            int bi = (int)(bitPos % 64);
            ef._lowWords[wi] |= low << bi;
            if (bi > 0 && (ulong)bi + ef._lowBits > 64) ef._lowWords[wi + 1] |= low >> (64 - bi);
        }
        return ef;
    }

    /// <summary>Access: the i-th value.</summary>
    public ulong Access(ulong i)
    {
        ulong high = _high.FindNthOneBit(i) - i;
        ulong low = 0;
        if (_lowBits > 0)
        {
            ulong bitPos = i * _lowBits;
            int wi = (int)(bitPos / 64);
            int bi = (int)(bitPos % 64);
            low = (_lowWords[wi] >> bi) & _lowMask;
            if (bi > 0 && (ulong)bi + _lowBits > 64)
            {
                int rem = (int)((ulong)bi + _lowBits - 64);
                low |= (_lowWords[wi + 1] & ((1UL << rem) - 1)) << (64 - bi);
            }
        }
        return (high << (int)_lowBits) | low;
    }

    public ulong SerializedSize => 24 + _high.SerializedSize + (ulong)_lowWords.Length * 8;

    public void EncodeTo(IBufferWriter<byte> w)
    {
        Coding.PutFixed64(w, _count);
        Coding.PutFixed64(w, _universe);
        Coding.PutFixed64(w, _lowBits);
        _high.EncodeTo(w);
        foreach (ulong v in _lowWords) Coding.PutFixed64(w, v);
    }

    public static EliasFano Read(ReadOnlySpan<byte> data, ref int pos)
    {
        if (data.Length - pos < 24) ThrowHelper.Corruption("EliasFano: insufficient data for header");
        var ef = new EliasFano
        {
            _count = Coding.DecodeFixed64(data[pos..]),
            _universe = Coding.DecodeFixed64(data[(pos + 8)..]),
            _lowBits = Coding.DecodeFixed64(data[(pos + 16)..]),
        };
        int p = pos + 24;
        if (ef._lowBits > 63) ThrowHelper.Corruption("EliasFano: low_bits exceeds 63");
        if (ef._count > 1UL << 30) ThrowHelper.Corruption("EliasFano: count exceeds reasonable limit");
        ef._lowMask = (1UL << (int)ef._lowBits) - 1;
        ef._high = Bitvector.Read(data, ref p);
        ulong numLowWords = (ef._count * ef._lowBits + 63) / 64;
        ulong lowBytes = numLowWords * 8;
        if ((ulong)(data.Length - p) < lowBytes) ThrowHelper.Corruption("EliasFano: insufficient data for low words");
        if (lowBytes > 0 && p % 8 != 0) ThrowHelper.Corruption("EliasFano: low words not 8-byte aligned");
        ef._lowWords = TrieBits.ReadUInt64s(data[p..], (int)numLowWords);
        pos = p + (int)lowBytes;
        return ef;
    }
}
