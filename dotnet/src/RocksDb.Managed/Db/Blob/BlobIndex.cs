using System.Buffers;
using System.Globalization;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>BlobIndex::Type (db/blob/blob_index.h). The byte values are persisted.</summary>
internal enum BlobIndexType : byte
{
    InlinedTtl = 0,
    Blob = 1,
    BlobTtl = 2,
    Unknown = 3,
}

/// <summary>
/// The value of a <c>kTypeBlobIndex</c> entry: a reference to a blob in a blob file (file number, offset of the blob
/// value, size, compression), optionally with an expiration (TTL), or an inlined value with an expiration (stacked
/// BlobDB). Port of rocksdb::BlobIndex (db/blob/blob_index.h). A <c>ref struct</c> because an inlined index points into
/// the decoded input.
/// <code>
/// kBlob:       type(1) file_number(varint64) offset(varint64) size(varint64) compression(1)
/// kBlobTTL:    type(1) expiration(varint64) file_number offset size compression
/// kInlinedTTL: type(1) expiration(varint64) value(rest)
/// </code>
/// </summary>
internal readonly ref struct BlobIndex
{
    /// <summary>kCurrentFileBlobIndexFileNumber (blob_constants.h): a blob embedded in the same file (not supported).</summary>
    public const ulong CurrentFileBlobIndexFileNumber = 0;
    /// <summary>Upper bound of an encoded non-inlined index (kMaxVarint64Length * 4 + 2).</summary>
    public const int MaxEncodedLength = Coding.MaxVarint64Length * 4 + 2;

    public BlobIndexType Type { get; init; }
    public ulong Expiration { get; init; }
    public ReadOnlySpan<byte> InlinedValue { get; init; }
    public ulong FileNumber { get; init; }
    public ulong Offset { get; init; }
    public ulong Size { get; init; }
    public CompressionType Compression { get; init; }

    public bool IsInlined => Type == BlobIndexType.InlinedTtl;
    public bool HasTtl => Type is BlobIndexType.InlinedTtl or BlobIndexType.BlobTtl;
    /// <summary>IsSameFile: a reference to a blob embedded in the same table file.</summary>
    public bool IsSameFile => Type is BlobIndexType.Blob or BlobIndexType.BlobTtl && FileNumber == CurrentFileBlobIndexFileNumber;

    /// <summary>BlobIndex::DecodeFrom. Returns null on success or the Corruption message.</summary>
    public static string? TryDecode(ReadOnlySpan<byte> slice, out BlobIndex index)
    {
        const string Msg = "Error while decoding blob index: ";
        index = default;
        if (slice.IsEmpty) return Msg + "Unknown blob index type: empty";
        var type = (BlobIndexType)slice[0];
        if (type >= BlobIndexType.Unknown)
        {
            return Msg + "Unknown blob index type: " + ((sbyte)slice[0]).ToString(CultureInfo.InvariantCulture);
        }
        slice = slice[1..];
        ulong expiration = 0;
        if (type is BlobIndexType.InlinedTtl or BlobIndexType.BlobTtl)
        {
            if (!Coding.GetVarint64(ref slice, out expiration)) return Msg + "Corrupted expiration";
        }
        if (type == BlobIndexType.InlinedTtl)
        {
            index = new BlobIndex { Type = type, Expiration = expiration, InlinedValue = slice };
            return null;
        }
        if (Coding.GetVarint64(ref slice, out ulong fileNumber) && Coding.GetVarint64(ref slice, out ulong offset) &&
            Coding.GetVarint64(ref slice, out ulong size) && slice.Length == 1)
        {
            index = new BlobIndex
            {
                Type = type,
                Expiration = expiration,
                FileNumber = fileNumber,
                Offset = offset,
                Size = size,
                Compression = (CompressionType)slice[0],
            };
            return null;
        }
        return Msg + "Corrupted blob offset";
    }

    /// <summary>Decodes or throws Corruption.</summary>
    public static BlobIndex Decode(ReadOnlySpan<byte> slice)
    {
        string? err = TryDecode(slice, out BlobIndex index);
        if (err is not null) ThrowHelper.Corruption(err);
        return index;
    }

    /// <summary>BlobIndex::EncodeBlob into <paramref name="dst"/> (at least <see cref="MaxEncodedLength"/> bytes); returns the length.</summary>
    public static int EncodeBlob(Span<byte> dst, ulong fileNumber, ulong offset, ulong size, CompressionType compression)
    {
        dst[0] = (byte)BlobIndexType.Blob;
        int n = 1;
        n += Coding.EncodeVarint64(dst[n..], fileNumber);
        n += Coding.EncodeVarint64(dst[n..], offset);
        n += Coding.EncodeVarint64(dst[n..], size);
        dst[n++] = (byte)compression;
        return n;
    }

    /// <summary>BlobIndex::EncodeBlob appended to <paramref name="dst"/>.</summary>
    public static void EncodeBlob(IBufferWriter<byte> dst, ulong fileNumber, ulong offset, ulong size, CompressionType compression) =>
        dst.Advance(EncodeBlob(dst.GetSpan(MaxEncodedLength), fileNumber, offset, size, compression));

    /// <summary>BlobIndex::EncodeBlobTTL appended to <paramref name="dst"/>.</summary>
    public static void EncodeBlobTtl(IBufferWriter<byte> dst, ulong expiration, ulong fileNumber, ulong offset, ulong size, CompressionType compression)
    {
        Span<byte> s = dst.GetSpan(MaxEncodedLength);
        s[0] = (byte)BlobIndexType.BlobTtl;
        int n = 1;
        n += Coding.EncodeVarint64(s[n..], expiration);
        n += Coding.EncodeVarint64(s[n..], fileNumber);
        n += Coding.EncodeVarint64(s[n..], offset);
        n += Coding.EncodeVarint64(s[n..], size);
        s[n++] = (byte)compression;
        dst.Advance(n);
    }

    /// <summary>BlobIndex::EncodeInlinedTTL appended to <paramref name="dst"/>.</summary>
    public static void EncodeInlinedTtl(IBufferWriter<byte> dst, ulong expiration, ReadOnlySpan<byte> value)
    {
        Span<byte> s = dst.GetSpan(1 + Coding.MaxVarint64Length);
        s[0] = (byte)BlobIndexType.InlinedTtl;
        int n = 1 + Coding.EncodeVarint64(s[1..], expiration);
        dst.Advance(n);
        dst.Write(value);
    }

    /// <summary>BlobIndex::DebugString (without hex output).</summary>
    public string DebugString()
    {
        var sb = new System.Text.StringBuilder();
        if (IsInlined)
        {
            sb.Append("[inlined blob] value:").Append(System.Text.Encoding.Latin1.GetString(InlinedValue));
        }
        else
        {
            sb.Append("[blob ref] file:").Append(IsSameFile ? "same" : FileNumber.ToString(CultureInfo.InvariantCulture))
              .Append(" offset:").Append(Offset.ToString(CultureInfo.InvariantCulture))
              .Append(" size:").Append(Size.ToString(CultureInfo.InvariantCulture))
              .Append(" compression: ").Append(Compression.ToString());
        }
        if (HasTtl) sb.Append(" exp:").Append(Expiration.ToString(CultureInfo.InvariantCulture));
        return sb.ToString();
    }
}
