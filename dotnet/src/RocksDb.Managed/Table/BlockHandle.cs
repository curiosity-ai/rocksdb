using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Pointer to the extent of a file that stores a data block or a meta block.
/// Port of <c>BlockHandle</c> (table/format.h, table/format.cc). Encoded as varint64 offset + varint64 size.
/// <see cref="Size"/> excludes the 5-byte block trailer.
/// </summary>
public readonly record struct BlockHandle(ulong Offset, ulong Size)
{
    /// <summary>kMaxEncodedLength = 2 * kMaxVarint64Length.</summary>
    public const int MaxEncodedLength = 2 * Coding.MaxVarint64Length;

    /// <summary>The null handle (offset 0, size 0), e.g. "no index handle in footer" for format_version &gt;= 6.</summary>
    public static BlockHandle Null => default;

    public bool IsNull => Offset == 0 && Size == 0;

    /// <summary>Writes the encoding into <paramref name="dst"/> (needs up to <see cref="MaxEncodedLength"/> bytes); returns bytes written.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int EncodeTo(Span<byte> dst)
    {
        int n = Coding.EncodeVarint64(dst, Offset);
        return n + Coding.EncodeVarint64(dst[n..], Size);
    }

    public void EncodeTo(IBufferWriter<byte> w) => w.Advance(EncodeTo(w.GetSpan(MaxEncodedLength)));

    /// <summary>Port of BlockHandle::DecodeFrom; advances <paramref name="input"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryDecode(ref ReadOnlySpan<byte> input, out BlockHandle handle)
    {
        if (Coding.GetVarint64(ref input, out ulong offset) && Coding.GetVarint64(ref input, out ulong size))
        {
            handle = new BlockHandle(offset, size);
            return true;
        }
        handle = default;
        return false;
    }

    /// <summary>Decodes a handle that must be well-formed; throws Corruption otherwise.</summary>
    public static BlockHandle Decode(ReadOnlySpan<byte> input)
    {
        if (!TryDecode(ref input, out BlockHandle h)) ThrowHelper.Corruption("bad block handle");
        return h;
    }

    public override string ToString() => "BlockHandle(offset=" + Offset + ", size=" + Size + ")";
}

/// <summary>
/// Value stored in index blocks: a <see cref="BlockHandle"/> optionally followed by the block's first internal key
/// (kBinarySearchWithFirstKey). Port of <c>IndexValue</c> (table/format.h, table/format.cc).
/// With delta encoding (format_version &gt;= 4) a non-restart entry stores only the signed size delta against the
/// previous handle, and the offset is implied as contiguous (previous offset + size + block trailer).
/// </summary>
internal static class IndexValue
{
    /// <summary>Port of IndexValue::EncodeTo without a previous handle.</summary>
    public static void EncodeFull(IBufferWriter<byte> dst, BlockHandle handle, bool haveFirstKey, ReadOnlySpan<byte> firstInternalKey)
    {
        handle.EncodeTo(dst);
        if (haveFirstKey) Coding.PutLengthPrefixedSlice(dst, firstInternalKey);
    }

    /// <summary>
    /// Port of IndexValue::EncodeTo with a previous handle. Without the escape (format_version 4..7) the handle must be
    /// contiguous and only the zigzag size delta is stored. With <paramref name="useValueDeltaEscape"/>
    /// (format_version &gt;= 8) a contiguous handle stores zigzag(delta) + 1 and a non-contiguous one stores varint 0
    /// followed by the full handle.
    /// </summary>
    public static void EncodeDelta(IBufferWriter<byte> dst, BlockHandle handle, BlockHandle previous, bool useValueDeltaEscape, bool haveFirstKey,
        ReadOnlySpan<byte> firstInternalKey)
    {
        // WART (upstream): specific to block-based table.
        ulong contiguousOffset = previous.Offset + previous.Size + BlockBasedTableConstants.BlockTrailerSize;
        if (useValueDeltaEscape)
        {
            if (handle.Offset == contiguousOffset)
            {
                Coding.PutVarint64(dst, Coding.ZigZagEncode64((long)handle.Size - (long)previous.Size) + 1);
            }
            else
            {
                // Escape: a full BlockHandle (offset + size) follows.
                Coding.PutVarint64(dst, 0);
                handle.EncodeTo(dst);
            }
        }
        else
        {
            System.Diagnostics.Debug.Assert(handle.Offset == contiguousOffset);
            Coding.PutVarint64(dst, Coding.ZigZagEncode64((long)handle.Size - (long)previous.Size));
        }
        if (haveFirstKey) Coding.PutLengthPrefixedSlice(dst, firstInternalKey);
    }

    /// <summary>
    /// Port of IndexValue::DecodeFrom. When <paramref name="previous"/> is non-null the delta form is decoded.
    /// <paramref name="firstKeyOffset"/>/<paramref name="firstKeyLength"/> describe the first key relative to the
    /// start of <paramref name="input"/> as it was passed in (only when <paramref name="haveFirstKey"/>).
    /// </summary>
    public static bool TryDecode(ref ReadOnlySpan<byte> input, bool haveFirstKey, bool hasPrevious, BlockHandle previous, bool useValueDeltaEscape,
        out BlockHandle handle, out int firstKeyOffset, out int firstKeyLength)
    {
        int startLen = input.Length;
        firstKeyOffset = 0;
        firstKeyLength = 0;
        if (hasPrevious)
        {
            if (!Coding.GetVarint64(ref input, out ulong zz))
            {
                handle = default;
                return false;
            }
            if (useValueDeltaEscape && zz == 0)
            {
                // Escape: a full BlockHandle (offset + size) follows.
                if (!BlockHandle.TryDecode(ref input, out handle)) return false;
            }
            else
            {
                // format_version >= 8 stores zigzag(delta) + 1 to keep 0 free for the escape.
                if (useValueDeltaEscape) zz--;
                long delta = Coding.ZigZagDecode64(zz);
                handle = new BlockHandle(previous.Offset + previous.Size + BlockBasedTableConstants.BlockTrailerSize, (ulong)((long)previous.Size + delta));
            }
        }
        else if (!BlockHandle.TryDecode(ref input, out handle))
        {
            return false;
        }
        if (haveFirstKey)
        {
            if (!Coding.GetVarint32(ref input, out uint len) || (uint)input.Length < len) return false;
            firstKeyOffset = startLen - input.Length;
            firstKeyLength = (int)len;
            input = input[(int)len..];
        }
        return true;
    }
}
