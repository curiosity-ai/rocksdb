using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Port of util/coding.h. All fixed-width integers are little-endian on disk regardless of host
/// endianness (matching RocksDB's EncodeFixed*/DecodeFixed*). Varints are LEB128 (7 bits per byte).
/// Decoders operate on a <c>ref ReadOnlySpan&lt;byte&gt;</c> "cursor" and advance it, mirroring
/// GetVarint32(Slice*), and return false on truncated/malformed input (never throw).
/// </summary>
public static class Coding
{
    public const int MaxVarint32Length = 5;
    public const int MaxVarint64Length = 10;

    // ---- Fixed -----------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void EncodeFixed16(Span<byte> dst, ushort value) => BinaryPrimitives.WriteUInt16LittleEndian(dst, value);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void EncodeFixed32(Span<byte> dst, uint value) => BinaryPrimitives.WriteUInt32LittleEndian(dst, value);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void EncodeFixed64(Span<byte> dst, ulong value) => BinaryPrimitives.WriteUInt64LittleEndian(dst, value);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ushort DecodeFixed16(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadUInt16LittleEndian(src);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint DecodeFixed32(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadUInt32LittleEndian(src);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong DecodeFixed64(ReadOnlySpan<byte> src) => BinaryPrimitives.ReadUInt64LittleEndian(src);

    public static void PutFixed16(IBufferWriter<byte> w, ushort v) { EncodeFixed16(w.GetSpan(2), v); w.Advance(2); }
    public static void PutFixed32(IBufferWriter<byte> w, uint v) { EncodeFixed32(w.GetSpan(4), v); w.Advance(4); }
    public static void PutFixed64(IBufferWriter<byte> w, ulong v) { EncodeFixed64(w.GetSpan(8), v); w.Advance(8); }

    public static bool GetFixed16(ref ReadOnlySpan<byte> input, out ushort value)
    {
        if (input.Length < 2) { value = 0; return false; }
        value = DecodeFixed16(input); input = input[2..]; return true;
    }

    public static bool GetFixed32(ref ReadOnlySpan<byte> input, out uint value)
    {
        if (input.Length < 4) { value = 0; return false; }
        value = DecodeFixed32(input); input = input[4..]; return true;
    }

    public static bool GetFixed64(ref ReadOnlySpan<byte> input, out ulong value)
    {
        if (input.Length < 8) { value = 0; return false; }
        value = DecodeFixed64(input); input = input[8..]; return true;
    }

    // ---- Varint ----------------------------------------------------------------------------

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int VarintLength(ulong v)
    {
        // ceil(bits/7), with 0 -> 1
        int bits = 64 - System.Numerics.BitOperations.LeadingZeroCount(v | 1);
        return (bits + 6) / 7;
    }

    /// <summary>Writes a varint32 into <paramref name="dst"/>; returns bytes written. dst must have room (<= 5).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int EncodeVarint32(Span<byte> dst, uint v)
    {
        int i = 0;
        while (v >= 0x80)
        {
            dst[i++] = (byte)(v | 0x80);
            v >>= 7;
        }
        dst[i++] = (byte)v;
        return i;
    }

    /// <summary>Writes a varint64 into <paramref name="dst"/>; returns bytes written. dst must have room (<= 10).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int EncodeVarint64(Span<byte> dst, ulong v)
    {
        int i = 0;
        while (v >= 0x80)
        {
            dst[i++] = (byte)(v | 0x80);
            v >>= 7;
        }
        dst[i++] = (byte)v;
        return i;
    }

    public static void PutVarint32(IBufferWriter<byte> w, uint v) => w.Advance(EncodeVarint32(w.GetSpan(MaxVarint32Length), v));
    public static void PutVarint64(IBufferWriter<byte> w, ulong v) => w.Advance(EncodeVarint64(w.GetSpan(MaxVarint64Length), v));

    public static void PutVarint32Varint32(IBufferWriter<byte> w, uint a, uint b)
    {
        Span<byte> s = w.GetSpan(2 * MaxVarint32Length);
        int n = EncodeVarint32(s, a);
        n += EncodeVarint32(s[n..], b);
        w.Advance(n);
    }

    /// <summary>PutLengthPrefixedSlice: varint32 length followed by the bytes.</summary>
    public static void PutLengthPrefixedSlice(IBufferWriter<byte> w, ReadOnlySpan<byte> value)
    {
        PutVarint32(w, (uint)value.Length);
        w.Write(value);
    }

    /// <summary>Decodes a varint32 from the start of <paramref name="input"/> without advancing. Returns bytes consumed, or 0 on error.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int DecodeVarint32(ReadOnlySpan<byte> input, out uint value)
    {
        // Fast path for the overwhelmingly common 1-byte case.
        if (input.Length > 0 && input[0] < 0x80)
        {
            value = input[0];
            return 1;
        }
        return DecodeVarint32Slow(input, out value);
    }

    private static int DecodeVarint32Slow(ReadOnlySpan<byte> input, out uint value)
    {
        uint result = 0;
        int limit = Math.Min(input.Length, MaxVarint32Length);
        for (int i = 0, shift = 0; i < limit; i++, shift += 7)
        {
            uint b = input[i];
            if ((b & 0x80) != 0)
            {
                result |= (b & 0x7F) << shift;
            }
            else
            {
                result |= b << shift;
                value = result;
                return i + 1;
            }
        }
        value = 0;
        return 0;
    }

    /// <summary>Decodes a varint64 without advancing. Returns bytes consumed, or 0 on error.</summary>
    public static int DecodeVarint64(ReadOnlySpan<byte> input, out ulong value)
    {
        ulong result = 0;
        int limit = Math.Min(input.Length, MaxVarint64Length);
        for (int i = 0, shift = 0; i < limit; i++, shift += 7)
        {
            ulong b = input[i];
            if ((b & 0x80) != 0)
            {
                result |= (b & 0x7F) << shift;
            }
            else
            {
                result |= b << shift;
                value = result;
                return i + 1;
            }
        }
        value = 0;
        return 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool GetVarint32(ref ReadOnlySpan<byte> input, out uint value)
    {
        int n = DecodeVarint32(input, out value);
        if (n == 0) return false;
        input = input[n..];
        return true;
    }

    public static bool GetVarint64(ref ReadOnlySpan<byte> input, out ulong value)
    {
        int n = DecodeVarint64(input, out value);
        if (n == 0) return false;
        input = input[n..];
        return true;
    }

    /// <summary>Signed varint (zig-zag), as in util/coding.h PutVarsignedint64.</summary>
    public static ulong ZigZagEncode64(long v) => (ulong)((v << 1) ^ (v >> 63));
    public static long ZigZagDecode64(ulong v) => (long)(v >> 1) ^ -(long)(v & 1);

    public static bool GetLengthPrefixedSlice(ref ReadOnlySpan<byte> input, out ReadOnlySpan<byte> result)
    {
        if (GetVarint32(ref input, out uint len) && (uint)input.Length >= len)
        {
            result = input[..(int)len];
            input = input[(int)len..];
            return true;
        }
        result = default;
        return false;
    }
}
