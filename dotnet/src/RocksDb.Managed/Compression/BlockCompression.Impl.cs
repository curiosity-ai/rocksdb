using System.Buffers;
using System.IO.Compression;
using K4os.Compression.LZ4;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Compression;

/// <summary>
/// Implementation of the <see cref="BlockCompression"/> contract. Port of the built-in V2 compressors and
/// decompressor in util/compression.cc (BuiltinSnappyCompressorV2, BuiltinZlibCompressorV2,
/// BuiltinLZ4CompressorV2NoDict, BuiltinLZ4HCCompressorV2, BuiltinZSTDCompressorV2, StartCompressBlockV2,
/// Decompressor::ExtractUncompressedSize, *_DecompressBlock) and the block builder's acceptance rule
/// (block_based_table_builder.cc: compressed size must be &lt;= max_compressed_bytes_per_kb * raw / 1024).
/// <para>Framing ("compression format 2", format_version &gt;= 2): every algorithm except Snappy is prefixed with
/// the varint32 uncompressed size; Snappy is raw snappy (which carries its own varint length).</para>
/// <list type="bullet">
/// <item>Snappy: Snappier (raw snappy block format).</item>
/// <item>LZ4/LZ4HC: raw LZ4 block format (not the frame format). Both types share one wire format; the level picks fast
///   vs HC exactly like upstream. Fast LZ4 below 64 KiB uses <see cref="Lz4BlockEncoder"/> (a port of liblz4's
///   LZ4_compress_generic byU16 path, same output as native); larger inputs, LZ4HC and decompression use K4os. LZ4 fast
///   acceleration &gt; 1 (level &lt; -1) is not supported, so it compresses with acceleration 1 (output still fully
///   compatible).</item>
/// <item>ZSTD: ZstdSharp, a plain zstd frame (ZSTD_compress2) with pooled thread-local contexts.</item>
/// <item>Zlib: raw deflate (window_bits = -14). .NET's deflater (both target frameworks) uses a 32 KiB window,
///   which a native inflater with window_bits -14 rejects as soon as a match reaches back more than 16 KiB. Inputs
///   up to 16 KiB are one deflate stream; larger inputs are deflated in independent 16 KiB chunks, each ending
///   with a sync flush (byte aligned, no final block) and starting with an empty history, so no match crosses a
///   chunk start; an empty final fixed-Huffman block (0x03 0x00) ends the stream. Any inflater with a 16 KiB
///   window reads it (the ratio is slightly worse than zlib's own, the format is the same). Decompression accepts
///   any raw deflate stream. Zlib (de)compression allocates a stream object per call / chunk (rarely used codec).</item>
/// <item>BZip2/Xpress: NotSupported.</item>
/// </list>
/// Dictionary compression is not supported yet.
/// </summary>
public static partial class BlockCompression
{
    /// <summary>CompressionOptions::max_compressed_bytes_per_kb default (1024 * 7 / 8): require at least 12.5% savings.</summary>
    public const int DefaultMaxCompressedBytesPerKb = 1024 * 7 / 8;

    /// <summary>CompressionOptions::kDefaultCompressionLevel.</summary>
    public const int DefaultCompressionLevel = 32767;

    private const int ZstdDefaultLevel = 3; // ZSTD_CLEVEL_DEFAULT
    private const int Lz4HcDefaultLevel = 9; // LZ4HC_CLEVEL_DEFAULT
    private const int Lz4HcMaxLevel = 12; // LZ4HC_CLEVEL_MAX
    private const int ZlibMaxBlockSize = 1 << 14; // see class remarks (window_bits = -14)

    [ThreadStatic] private static ZstdSharp.Compressor? t_zstdCompressor;
    [ThreadStatic] private static ZstdSharp.Decompressor? t_zstdDecompressor;

    public static partial bool IsSupported(CompressionType type) => type switch
    {
        CompressionType.None or CompressionType.Snappy or CompressionType.Zlib or CompressionType.LZ4
            or CompressionType.LZ4HC or CompressionType.Zstd => true,
        _ => false,
    };

    /// <summary>
    /// GoodCompressionRatio: whether a block compressed to <paramref name="compressedSize"/> bytes (including the
    /// size prefix) is worth storing compressed, for the given max_compressed_bytes_per_kb.
    /// </summary>
    public static bool IsGoodCompressionRatio(int compressedSize, int rawSize, int maxCompressedBytesPerKb)
        => maxCompressedBytesPerKb > 0 && (ulong)compressedSize <= MaxCompressedSize(rawSize, maxCompressedBytesPerKb);

    /// <summary>GoodCompressionRatio with the default threshold (compressed &lt;= raw * 7/8).</summary>
    public static bool IsGoodCompressionRatio(int compressedSize, int rawSize)
        => IsGoodCompressionRatio(compressedSize, rawSize, DefaultMaxCompressedBytesPerKb);

    private static ulong MaxCompressedSize(int rawSize, int maxCompressedBytesPerKb)
        => ((ulong)Math.Min(maxCompressedBytesPerKb, 1023) * (ulong)rawSize) >> 10;

    public static partial bool TryCompress(CompressionType type, ReadOnlySpan<byte> raw, int formatVersion, int level, IBufferWriter<byte> output)
        => TryCompress(type, raw, formatVersion, level, DefaultMaxCompressedBytesPerKb, output);

    /// <summary>
    /// Like <see cref="TryCompress(CompressionType, ReadOnlySpan{byte}, int, int, IBufferWriter{byte})"/> with an explicit
    /// <c>max_compressed_bytes_per_kb</c> acceptance threshold (values &gt; 1023 are capped like the table builder does).
    /// Nothing is written to <paramref name="output"/> when this returns false. <paramref name="level"/> follows
    /// CompressionOptions::level semantics (<see cref="DefaultCompressionLevel"/> = library default).
    /// </summary>
    public static bool TryCompress(CompressionType type, ReadOnlySpan<byte> raw, int formatVersion, int level, int maxCompressedBytesPerKb, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        if (maxCompressedBytesPerKb <= 0 || type == CompressionType.None)
        {
            return false;
        }
        ulong maxSize64 = MaxCompressedSize(raw.Length, maxCompressedBytesPerKb);
        int maxSize = (int)Math.Min(maxSize64, int.MaxValue);
        if (type == CompressionType.Snappy)
        {
            return TryCompressSnappy(raw, maxSize, output);
        }
        if (formatVersion < 2)
        {
            // compression format 1 is never written by supported table format versions
            return false;
        }
        // StartCompressBlockV2: need enough output space for encoding uncompressed size
        if (maxSize <= 5)
        {
            return false;
        }
        return type switch
        {
            CompressionType.LZ4 or CompressionType.LZ4HC => TryCompressLz4(type, raw, level, maxSize, output),
            CompressionType.Zstd => TryCompressZstd(raw, level, maxSize, output),
            CompressionType.Zlib => TryCompressZlib(raw, level, maxSize, output),
            _ => false, // BZip2, Xpress, unknown: unsupported -> store uncompressed
        };
    }

    /// <summary>
    /// Port of LegacyForceBuiltinCompression (util/compression.cc), used for blob values: compresses with format-2
    /// framing and keeps the result even when it is larger than the input. Throws NotSupported when the type cannot be
    /// produced (BZip2/Xpress).
    /// </summary>
    internal static void CompressForced(CompressionType type, ReadOnlySpan<byte> raw, int level, IBufferWriter<byte> output)
    {
        // Reliable upper bound on the compressed size: ((19 * n) >> 4) + 604.
        long bound = ((19L * raw.Length) >> 4) + 604;
        int maxSize = (int)Math.Min(bound, int.MaxValue);
        bool ok = type switch
        {
            CompressionType.Snappy => TryCompressSnappy(raw, int.MaxValue, output),
            CompressionType.LZ4 or CompressionType.LZ4HC => TryCompressLz4(type, raw, level, int.MaxValue, output),
            CompressionType.Zstd => TryCompressZstd(raw, level, maxSize, output),
            CompressionType.Zlib => TryCompressZlib(raw, level, maxSize, output),
            _ => false,
        };
        if (!ok) ThrowHelper.NotSupported("Compression type " + type.ToString() + " cannot compress this blob in this build");
    }

    private static bool TryCompressSnappy(ReadOnlySpan<byte> raw, int maxSize, IBufferWriter<byte> output)
    {
        Span<byte> dst = output.GetSpan(Snappier.Snappy.GetMaxCompressedLength(raw.Length));
        if (!Snappier.Snappy.TryCompress(raw, dst, out int written) || written <= 0 || written > maxSize)
        {
            return false;
        }
        output.Advance(written);
        return true;
    }

    /// <summary>
    /// Benchmark A/B switch (TableBuildBenchmarks): true routes fast LZ4 below 64 KiB through K4os instead of
    /// <see cref="Lz4BlockEncoder"/>. The output is LZ4-compatible either way. Not for production use.
    /// </summary>
    internal static bool UseK4osLz4Encoder;

    private static bool TryCompressLz4(CompressionType type, ReadOnlySpan<byte> raw, int level, int maxSize, IBufferWriter<byte> output)
    {
        // LZ4 and LZ4HC share the same wire format; level selects the compressor (BuiltinCompressionManagerV2::GetCompressor)
        bool useHc = level == DefaultCompressionLevel ? type == CompressionType.LZ4HC : level >= 1;
        LZ4Level lz4Level = LZ4Level.L00_FAST;
        if (useHc)
        {
            int hc = level == DefaultCompressionLevel ? Lz4HcDefaultLevel : Math.Min(level, Lz4HcMaxLevel);
            lz4Level = hc <= 3 ? LZ4Level.L03_HC : (LZ4Level)hc;
        }
        int bound = LZ4Codec.MaximumOutputSize(raw.Length);
        Span<byte> dst = output.GetSpan(Coding.MaxVarint32Length + bound);
        int header = Coding.EncodeVarint32(dst, (uint)raw.Length);
        int outLen = !useHc && raw.Length < Lz4BlockEncoder.Limit64K && BitConverter.IsLittleEndian && !UseK4osLz4Encoder
            // LZ4_compress_fast (acceleration 1) with the output limited to the acceptable size: 0 = rejected.
            ? Lz4BlockEncoder.Compress(raw, dst.Slice(header, (int)Math.Min(bound, (long)maxSize - header)))
            : LZ4Codec.Encode(raw, dst.Slice(header, bound), lz4Level);
        if (outLen <= 0 || header + outLen > maxSize)
        {
            return false;
        }
        output.Advance(header + outLen);
        return true;
    }

    private static int SanitizeZstdLevel(int level) => level == DefaultCompressionLevel ? ZstdDefaultLevel : level == 0 ? -1 : level;

    private static bool TryCompressZstd(ReadOnlySpan<byte> raw, int level, int maxSize, IBufferWriter<byte> output)
    {
        int zlevel = SanitizeZstdLevel(level);
        ZstdSharp.Compressor? c = t_zstdCompressor;
        if (c is null)
        {
            c = new ZstdSharp.Compressor(zlevel);
            t_zstdCompressor = c;
        }
        else if (c.Level != zlevel)
        {
            c.Level = zlevel;
        }
        Span<byte> dst = output.GetSpan(maxSize)[..maxSize];
        int header = Coding.EncodeVarint32(dst, (uint)raw.Length);
        bool ok;
        int outLen;
        try
        {
            ok = c.TryWrap(raw, dst[header..], out outLen);
        }
        catch (ZstdSharp.ZstdException)
        {
            // Context may be in a bad state; drop it.
            t_zstdCompressor = null;
            c.Dispose();
            return false;
        }
        if (!ok || outLen <= 0)
        {
            return false;
        }
        output.Advance(header + outLen);
        return true;
    }

    private static bool TryCompressZlib(ReadOnlySpan<byte> raw, int level, int maxSize, IBufferWriter<byte> output)
    {
        int zlevel = level == DefaultCompressionLevel ? -1 /* Z_DEFAULT_COMPRESSION */ : Math.Clamp(level, -1, 9);
        if (raw.Length > ZlibMaxBlockSize)
        {
            return TryCompressZlibChunked(raw, zlevel, maxSize, output);
        }
        byte[] buffer = ArrayPool<byte>.Shared.Rent(maxSize);
        try
        {
            int header = Coding.EncodeVarint32(buffer, (uint)raw.Length);
            var ms = new MemoryStream(buffer, header, maxSize - header, writable: true);
            try
            {
                using (var deflate = new DeflateStream(ms, new ZLibCompressionOptions { CompressionLevel = zlevel }, leaveOpen: true))
                {
                    deflate.Write(raw);
                }
            }
            catch (NotSupportedException)
            {
                // MemoryStream capacity exceeded: compressed output too large
                return false;
            }
            int total = header + (int)ms.Position;
            output.Write(buffer.AsSpan(0, total));
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Raw deflate of an input above 16 KiB that a 16 KiB-window inflater accepts (see the class remarks): independent
    /// 16 KiB chunks, each sync-flushed (the deflater's final block is discarded), then an empty final block.
    /// </summary>
    private static bool TryCompressZlibChunked(ReadOnlySpan<byte> raw, int zlevel, int maxSize, IBufferWriter<byte> output)
    {
        Span<byte> header = stackalloc byte[Coding.MaxVarint32Length];
        int headerLen = Coding.EncodeVarint32(header, (uint)raw.Length);
        int limit = maxSize - headerLen - 2;
        if (limit <= 0) return false;
        var ms = new MemoryStream(Math.Min(limit, raw.Length / 2 + 64));
        var options = new ZLibCompressionOptions { CompressionLevel = zlevel };
        for (int offset = 0; offset < raw.Length; offset += ZlibMaxBlockSize)
        {
            ReadOnlySpan<byte> chunk = raw.Slice(offset, Math.Min(ZlibMaxBlockSize, raw.Length - offset));
            var deflate = new DeflateStream(ms, options, leaveOpen: true);
            deflate.Write(chunk);
            deflate.Flush(); // Z_SYNC_FLUSH: everything so far, ending byte aligned with an empty stored block
            long end = ms.Position;
            deflate.Dispose(); // writes the (unwanted) final block after `end`: cut it off
            ms.SetLength(end);
            ms.Position = end;
            if (end > limit) return false;
        }
        // Empty final block with fixed Huffman codes: BFINAL=1, BTYPE=01, end-of-block code 0000000.
        ms.WriteByte(0x03);
        ms.WriteByte(0x00);
        output.Write(header[..headerLen]);
        output.Write(ms.GetBuffer().AsSpan(0, (int)ms.Length));
        return true;
    }

    // ---- Decompression -------------------------------------------------------------------------

    /// <summary>
    /// Returns the uncompressed size of a compressed block payload without decompressing it
    /// (Snappy: from its own header; others: the format-2 varint32 prefix). False on malformed input.
    /// </summary>
    public static bool TryGetUncompressedLength(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, out int length)
    {
        length = 0;
        switch (type)
        {
            case CompressionType.None:
                length = compressed.Length;
                return true;
            case CompressionType.Snappy:
                try
                {
                    length = Snappier.Snappy.GetUncompressedLength(compressed);
                    return length >= 0;
                }
                catch (InvalidDataException)
                {
                    return false;
                }
            default:
                if (formatVersion < 2)
                {
                    return false;
                }
                int n = Coding.DecodeVarint64(compressed, out ulong size);
                if (n == 0 || size > int.MaxValue)
                {
                    return false;
                }
                length = (int)size;
                return true;
        }
    }

    public static partial void Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        switch (type)
        {
            case CompressionType.None:
                output.Write(compressed);
                return;
            case CompressionType.Snappy:
                DecompressSnappy(compressed, output);
                return;
            case CompressionType.LZ4:
            case CompressionType.LZ4HC:
            case CompressionType.Zstd:
            case CompressionType.Zlib:
                break;
            default:
                ThrowNotSupported(type);
                return;
        }
        if (formatVersion < 2)
        {
            ThrowHelper.NotSupported("compression format 1 (format_version < 2)");
        }
        // Decompressor::ExtractUncompressedSize (historically varint32, generalized to varint64)
        int n = Coding.DecodeVarint64(compressed, out ulong size64);
        if (n == 0)
        {
            ThrowHelper.Corruption("Unable to extract uncompressed size");
        }
        if (size64 > int.MaxValue)
        {
            ThrowHelper.Corruption("Uncompressed size too large");
        }
        int size = (int)size64;
        ReadOnlySpan<byte> payload = compressed[n..];
        Span<byte> dst = output.GetSpan(size)[..size];
        switch (type)
        {
            case CompressionType.LZ4:
            case CompressionType.LZ4HC:
            {
                int got = LZ4Codec.Decode(payload, dst);
                if (got != size)
                {
                    ThrowHelper.Corruption(got < 0 ? "Error decompressing LZ4 data" : "Size mismatch decompressing LZ4 data");
                }
                break;
            }
            case CompressionType.Zstd:
                DecompressZstd(payload, dst);
                break;
            default:
                DecompressZlib(payload, dst);
                break;
        }
        output.Advance(size);
    }

    private static void ThrowNotSupported(CompressionType type)
        => ThrowHelper.NotSupported(type switch
        {
            CompressionType.BZip2 => "BZip2 not supported in this build",
            CompressionType.Xpress => "XPRESS not supported in this build",
            _ => "Unknown compression type",
        });

    private static void DecompressSnappy(ReadOnlySpan<byte> compressed, IBufferWriter<byte> output)
    {
        int len;
        try
        {
            len = Snappier.Snappy.GetUncompressedLength(compressed);
        }
        catch (InvalidDataException)
        {
            ThrowHelper.Corruption("Error decompressing snappy data");
            return;
        }
        Span<byte> dst = output.GetSpan(len)[..len];
        bool ok;
        int written;
        try
        {
            ok = Snappier.Snappy.TryDecompress(compressed, dst, out written);
        }
        catch (InvalidDataException)
        {
            ok = false;
            written = 0;
        }
        if (!ok || written != len)
        {
            ThrowHelper.Corruption("Error decompressing snappy data");
        }
        output.Advance(len);
    }

    private static void DecompressZstd(ReadOnlySpan<byte> payload, Span<byte> dst)
    {
        ZstdSharp.Decompressor d = t_zstdDecompressor ??= new ZstdSharp.Decompressor();
        int got;
        try
        {
            got = d.Unwrap(payload, dst);
        }
        catch (ZstdSharp.ZstdException e)
        {
            t_zstdDecompressor = null;
            d.Dispose();
            ThrowHelper.Corruption("ZSTD " + e.Message);
            return;
        }
        if (got != dst.Length)
        {
            ThrowHelper.Corruption("ZSTD decompression size mismatch");
        }
    }

    private static void DecompressZlib(ReadOnlySpan<byte> payload, Span<byte> dst)
    {
        byte[] input = ArrayPool<byte>.Shared.Rent(Math.Max(payload.Length, 1));
        try
        {
            payload.CopyTo(input);
            using var ms = new MemoryStream(input, 0, payload.Length, writable: false);
            using var inflate = new DeflateStream(ms, CompressionMode.Decompress);
            int total = 0;
            while (total < dst.Length)
            {
                int r = inflate.Read(dst[total..]);
                if (r == 0) break;
                total += r;
            }
            Span<byte> extra = stackalloc byte[1];
            if (total != dst.Length || inflate.Read(extra) != 0)
            {
                ThrowHelper.Corruption("Size mismatch decompressing zlib data");
            }
        }
        catch (InvalidDataException e)
        {
            ThrowHelper.Corruption("Failed zlib inflate: " + e.Message);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(input);
        }
    }
}
