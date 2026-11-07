using System.Buffers;
using System.IO.Compression;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Engine;
using RocksDb.Managed.Util;
using ZstdSharp.Unsafe;

namespace RocksDb.Managed.Compression;

/// <summary>
/// Dictionary compression of data blocks. Port of the dictionary paths of util/compression.cc:
/// BuiltinZSTDCompressorV2 (ZSTD_CCtx_refCDict + ZSTD_compress2), BuiltinLZ4CompressorV2WithDict (LZ4_loadDict +
/// LZ4_compress_fast_continue), CompressorWithSimpleDictBase for Snappy (dictionary stored but ignored), and the
/// decompressors (ZSTD_decompress_usingDDict, LZ4_decompress_safe_usingDict, inflateSetDictionary).
/// </summary>
/// <remarks>
/// Known deviations (output stays readable by native RocksDB): LZ4HC with a dictionary compresses with the fast LZ4
/// compressor (same wire format; .NET LZ4 has no HC dictionary API); zlib compresses without the dictionary (.NET's
/// deflate has no preset-dictionary API) - native inflates such blocks fine with its dictionary set. Reading native
/// zlib dictionary blocks is supported (the dictionary is replayed as a stored deflate block before the payload).
/// </remarks>
public static unsafe partial class BlockCompression
{
    [ThreadStatic] private static nint t_zstdDictCctx;
    [ThreadStatic] private static nint t_zstdDictDctx;

    /// <summary>
    /// Like <see cref="TryCompress(CompressionType, ReadOnlySpan{byte}, int, int, int, IBufferWriter{byte})"/> using
    /// <paramref name="dictionary"/> (null or empty: no dictionary).
    /// </summary>
    public static bool TryCompress(CompressionType type, ReadOnlySpan<byte> raw, int formatVersion, int level, int maxCompressedBytesPerKb,
        IBufferWriter<byte> output, CompressionDictionary? dictionary)
    {
        if (dictionary is null || dictionary.Length == 0 || type is not (CompressionType.Zstd or CompressionType.LZ4 or CompressionType.LZ4HC))
            return TryCompress(type, raw, formatVersion, level, maxCompressedBytesPerKb, output);
        ArgumentNullException.ThrowIfNull(output);
        if (maxCompressedBytesPerKb <= 0 || formatVersion < 2) return false;
        int maxSize = (int)Math.Min(MaxCompressedSize(raw.Length, maxCompressedBytesPerKb), int.MaxValue);
        // StartCompressBlockV2: need enough output space for encoding uncompressed size
        if (maxSize <= 5) return false;
        return type == CompressionType.Zstd
            ? TryCompressZstdDict(raw, level, maxSize, output, dictionary)
            : TryCompressLz4Dict(raw, maxSize, output, dictionary);
    }

    private static bool TryCompressZstdDict(ReadOnlySpan<byte> raw, int level, int maxSize, IBufferWriter<byte> output, CompressionDictionary dictionary)
    {
        int zlevel = SanitizeZstdLevel(level);
        ZSTD_CDict_s* cdict = dictionary.GetZstdCDict(zlevel);
        var cctx = (ZSTD_CCtx_s*)t_zstdDictCctx;
        if (cctx == null)
        {
            cctx = Methods.ZSTD_createCCtx();
            t_zstdDictCctx = (nint)cctx;
        }
        Methods.ZSTD_CCtx_reset(cctx, ZSTD_ResetDirective.ZSTD_reset_session_and_parameters);
        Methods.ZSTD_CCtx_setParameter(cctx, ZSTD_cParameter.ZSTD_c_compressionLevel, zlevel);
        Span<byte> dst = output.GetSpan(maxSize)[..maxSize];
        int header = Coding.EncodeVarint32(dst, (uint)raw.Length);
        nuint outLen;
        fixed (byte* d = dst)
        fixed (byte* s = raw)
        fixed (byte* rawDict = dictionary.Data)
        {
            if (cdict != null) Methods.ZSTD_CCtx_refCDict(cctx, cdict);
            else Methods.ZSTD_CCtx_loadDictionary(cctx, rawDict, (nuint)dictionary.Length);
            outLen = Methods.ZSTD_compress2(cctx, d + header, (nuint)(maxSize - header), s, (nuint)raw.Length);
        }
        if (Methods.ZSTD_isError(outLen)) return false; // dstSize_tooSmall: compression rejected
        output.Advance(header + (int)outLen);
        return true;
    }

    private static bool TryCompressLz4Dict(ReadOnlySpan<byte> raw, int maxSize, IBufferWriter<byte> output, CompressionDictionary dictionary)
    {
        // LZ4_loadDict + LZ4_compress_fast_continue: the dictionary is compressed first into a scratch buffer (only the
        // last 64 KiB matter, like LZ4_loadDict) so that the block may reference it as the preceding history.
        ReadOnlySpan<byte> dict = dictionary.Data;
        if (dict.Length > 64 * 1024) dict = dict[^(64 * 1024)..];
        Span<byte> dst = output.GetSpan(maxSize)[..maxSize];
        int header = Coding.EncodeVarint32(dst, (uint)raw.Length);
        byte[] scratch = RocksArrayPool<byte>.Shared.Rent(LZ4Codec.MaximumOutputSize(dict.Length));
        int outLen;
        try
        {
            using var ctx = new Pubternal.FastContext();
            fixed (byte* pd = dict)
            fixed (byte* ps = scratch)
            fixed (byte* src = raw)
            fixed (byte* pdst = dst)
            {
                if (dict.Length > 0) Pubternal.CompressFast(ctx, pd, ps, dict.Length, scratch.Length, 1);
                outLen = Pubternal.CompressFast(ctx, src, pdst + header, raw.Length, maxSize - header, 1);
            }
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(scratch);
        }
        if (outLen <= 0) return false;
        output.Advance(header + outLen);
        return true;
    }

    /// <summary>
    /// Like <see cref="Decompress(CompressionType, ReadOnlySpan{byte}, int, IBufferWriter{byte})"/> with the table's
    /// compression dictionary (null or empty: none).
    /// </summary>
    public static void Decompress(CompressionType type, ReadOnlySpan<byte> compressed, int formatVersion, IBufferWriter<byte> output,
        CompressionDictionary? dictionary)
    {
        if (dictionary is null || dictionary.Length == 0 || type is not (CompressionType.Zstd or CompressionType.LZ4 or CompressionType.LZ4HC or CompressionType.Zlib))
        {
            Decompress(type, compressed, formatVersion, output);
            return;
        }
        ArgumentNullException.ThrowIfNull(output);
        if (formatVersion < 2) ThrowHelper.NotSupported("compression format 1 (format_version < 2)");
        int n = Coding.DecodeVarint64(compressed, out ulong size64);
        if (n == 0) ThrowHelper.Corruption("Unable to extract uncompressed size");
        if (size64 > int.MaxValue) ThrowHelper.Corruption("Uncompressed size too large");
        int size = (int)size64;
        ReadOnlySpan<byte> payload = compressed[n..];
        Span<byte> dst = output.GetSpan(size)[..size];
        switch (type)
        {
            case CompressionType.Zstd:
                DecompressZstdDict(payload, dst, dictionary);
                break;
            case CompressionType.Zlib:
                DecompressZlibDict(payload, dst, dictionary.Data);
                break;
            default:
            {
                int got = LZ4Codec.Decode(payload, dst, dictionary.Data);
                if (got != size) ThrowHelper.Corruption(got < 0 ? "Error decompressing LZ4 data" : "Size mismatch decompressing LZ4 data");
                break;
            }
        }
        output.Advance(size);
    }

    private static void DecompressZstdDict(ReadOnlySpan<byte> payload, Span<byte> dst, CompressionDictionary dictionary)
    {
        var dctx = (ZSTD_DCtx_s*)t_zstdDictDctx;
        if (dctx == null)
        {
            dctx = Methods.ZSTD_createDCtx();
            t_zstdDictDctx = (nint)dctx;
        }
        ZSTD_DDict_s* ddict = dictionary.GetZstdDDict();
        nuint got;
        fixed (byte* d = dst)
        fixed (byte* s = payload)
        fixed (byte* rawDict = dictionary.Data)
        {
            got = ddict != null
                ? Methods.ZSTD_decompress_usingDDict(dctx, d, (nuint)dst.Length, s, (nuint)payload.Length, ddict)
                : Methods.ZSTD_decompress_usingDict(dctx, d, (nuint)dst.Length, s, (nuint)payload.Length, rawDict, (nuint)dictionary.Length);
        }
        if (Methods.ZSTD_isError(got))
        {
            ThrowHelper.Corruption("ZSTD " + Methods.ZSTD_getErrorName(got));
        }
        if (got != (nuint)dst.Length) ThrowHelper.Corruption("ZSTD decompression size mismatch");
    }

    /// <summary>
    /// Raw inflate with a preset dictionary (inflateSetDictionary): .NET has no such API, so the dictionary (at most the
    /// 2^14 byte window used by RocksDB's window_bits = -14, the last bytes of a longer one) is prepended as a
    /// non-final stored deflate block; back references of the payload then resolve into it.
    /// </summary>
    private static void DecompressZlibDict(ReadOnlySpan<byte> payload, Span<byte> dst, ReadOnlySpan<byte> dict)
    {
        const int window = 1 << 15; // inflate accepts distances up to its 32 KiB window
        if (dict.Length > window) dict = dict[^window..];
        int prefixLen = 5 + dict.Length;
        byte[] input = RocksArrayPool<byte>.Shared.Rent(prefixLen + payload.Length);
        byte[] output = RocksArrayPool<byte>.Shared.Rent(dict.Length + dst.Length + 1);
        try
        {
            // Stored block header: BFINAL=0, BTYPE=00, then LEN / NLEN (little endian) on the next byte boundary.
            input[0] = 0;
            input[1] = (byte)dict.Length;
            input[2] = (byte)(dict.Length >> 8);
            input[3] = (byte)~dict.Length;
            input[4] = (byte)(~dict.Length >> 8);
            dict.CopyTo(input.AsSpan(5));
            payload.CopyTo(input.AsSpan(prefixLen));
            using var ms = new MemoryStream(input, 0, prefixLen + payload.Length, writable: false);
            using var inflate = new DeflateStream(ms, CompressionMode.Decompress);
            int want = dict.Length + dst.Length;
            int total = 0;
            while (total < want + 1)
            {
                int r = inflate.Read(output, total, want + 1 - total);
                if (r == 0) break;
                total += r;
            }
            if (total != want) ThrowHelper.Corruption("Size mismatch decompressing zlib data");
            output.AsSpan(dict.Length, dst.Length).CopyTo(dst);
        }
        catch (InvalidDataException e)
        {
            ThrowHelper.Corruption("Failed zlib inflate: " + e.Message);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(input);
            RocksArrayPool<byte>.Shared.Return(output);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Dictionary construction

    /// <summary>
    /// Port of ZSTD_TrainDictionary (util/compression.cc): ZDICT_trainFromBuffer over the samples. Returns an empty
    /// array when training fails (upstream then writes no dictionary).
    /// </summary>
    public static byte[] ZstdTrainDictionary(ReadOnlySpan<byte> samples, ReadOnlySpan<nuint> sampleLengths, int maxDictBytes)
    {
        // Dictionary trainer is available since v0.6.1 for static linking, but not available for dynamic linking until
        // v1.1.3. For now we enable the feature in v1.1.3+ only.
        byte[] dict = new byte[maxDictBytes];
        nuint len;
        fixed (byte* d = dict)
        fixed (byte* s = samples)
        fixed (nuint* l = sampleLengths)
        {
            len = Methods.ZDICT_trainFromBuffer(d, (nuint)maxDictBytes, s, l, (uint)sampleLengths.Length);
        }
        if (Methods.ZDICT_isError(len)) return [];
        return dict.AsSpan(0, (int)len).ToArray();
    }

    /// <summary>
    /// Port of ZSTD_FinalizeDictionary (util/compression.cc): ZDICT_finalizeDictionary using the first
    /// min(samples, max_dict_bytes) sample bytes as dictionary content. Returns an empty array on failure.
    /// </summary>
    public static byte[] ZstdFinalizeDictionary(ReadOnlySpan<byte> samples, ReadOnlySpan<nuint> sampleLengths, int maxDictBytes, int level)
    {
        byte[] dict = new byte[maxDictBytes];
        ZDICT_params_t p = default;
        p.compressionLevel = SanitizeZstdLevel(level);
        nuint len;
        fixed (byte* d = dict)
        fixed (byte* s = samples)
        fixed (nuint* l = sampleLengths)
        {
            len = Methods.ZDICT_finalizeDictionary(d, (nuint)maxDictBytes, s, (nuint)Math.Min(samples.Length, maxDictBytes), s, l,
                (uint)sampleLengths.Length, p);
        }
        if (Methods.ZDICT_isError(len)) return [];
        return dict.AsSpan(0, (int)len).ToArray();
    }
}
