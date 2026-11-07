using ZstdSharp.Unsafe;

namespace RocksDb.Managed.Wal;

/// <summary>
/// Streaming ZSTD compressor used for WAL compression. Port of <c>ZSTDStreamingCompress</c> (util/compression.{h,cc}):
/// each logical record is compressed as one ZSTD frame (<c>ZSTD_e_end</c>, checksum flag on, default level), emitted
/// in chunks of at most <c>max_output_len</c> bytes so that each chunk fits in one physical record.
/// Not thread-safe.
/// </summary>
internal sealed unsafe class WalZstdCompressor : IDisposable
{
    private ZSTD_CCtx_s* _cctx;
    private int _inputPos;

    public WalZstdCompressor()
    {
        _cctx = Methods.ZSTD_createCCtx();
        if (_cctx == null) throw new OutOfMemoryException("ZSTD_createCCtx failed");
        Methods.ZSTD_CCtx_setParameter(_cctx, ZSTD_cParameter.ZSTD_c_checksumFlag, 1);
    }

    /// <summary>Call between logical records (frame boundaries) or after an error.</summary>
    public void Reset()
    {
        Methods.ZSTD_CCtx_reset(_cctx, ZSTD_ResetDirective.ZSTD_reset_session_only);
        _inputPos = 0;
    }

    /// <summary>
    /// Compresses more of <paramref name="input"/> (the same span must be passed until the frame is finished) into
    /// <paramref name="output"/>. Returns the number of bytes zstd still has to flush (0 = frame complete), or -1 on
    /// error. <paramref name="written"/> receives the produced byte count.
    /// </summary>
    public int Compress(ReadOnlySpan<byte> input, Span<byte> output, out int written)
    {
        written = 0;
        if (input.IsEmpty) return 0; // Don't need to compress an empty input
        fixed (byte* src = input)
        fixed (byte* dst = output)
        {
            var inBuf = new ZSTD_inBuffer_s { src = src, size = (nuint)input.Length, pos = (nuint)_inputPos };
            var outBuf = new ZSTD_outBuffer_s { dst = dst, size = (nuint)output.Length, pos = 0 };
            nuint remaining = Methods.ZSTD_compressStream2(_cctx, &outBuf, &inBuf, ZSTD_EndDirective.ZSTD_e_end);
            if (Methods.ZSTD_isError(remaining))
            {
                Reset();
                return -1;
            }
            _inputPos = (int)inBuf.pos;
            written = (int)outBuf.pos;
            return (int)remaining;
        }
    }

    public void Dispose()
    {
        if (_cctx != null)
        {
            Methods.ZSTD_freeCCtx(_cctx);
            _cctx = null;
        }
        GC.SuppressFinalize(this);
    }

    ~WalZstdCompressor() => Dispose();
}

/// <summary>
/// Streaming ZSTD decompressor for WAL records. Port of <c>ZSTDStreamingUncompress</c> (util/compression.cc).
/// Each physical record's payload is fed as a new input chunk; output is produced in pieces of at most
/// <see cref="LogFormat.BlockSize"/> bytes. Not thread-safe.
/// </summary>
internal sealed unsafe class WalZstdDecompressor : IDisposable
{
    private ZSTD_DCtx_s* _dctx;

    public WalZstdDecompressor()
    {
        _dctx = Methods.ZSTD_createDCtx();
        if (_dctx == null) throw new OutOfMemoryException("ZSTD_createDCtx failed");
    }

    public void Reset() => Methods.ZSTD_DCtx_reset(_dctx, ZSTD_ResetDirective.ZSTD_reset_session_only);

    /// <summary>
    /// Decompresses <paramref name="input"/> (a full physical record payload) appending all produced output to
    /// <paramref name="output"/>. Returns false on a decompression error (the context is reset).
    /// </summary>
    public bool Uncompress(ReadOnlySpan<byte> input, Util.PooledBufferWriter output)
    {
        if (input.IsEmpty) return true;
        fixed (byte* src = input)
        {
            var inBuf = new ZSTD_inBuffer_s { src = src, size = (nuint)input.Length, pos = 0 };
            while (true)
            {
                Span<byte> dstSpan = output.GetSpan(LogFormat.BlockSize)[..LogFormat.BlockSize];
                nuint produced;
                fixed (byte* dst = dstSpan)
                {
                    var outBuf = new ZSTD_outBuffer_s { dst = dst, size = (nuint)dstSpan.Length, pos = 0 };
                    nuint ret = Methods.ZSTD_decompressStream(_dctx, &outBuf, &inBuf);
                    if (Methods.ZSTD_isError(ret))
                    {
                        Reset();
                        return false;
                    }
                    produced = outBuf.pos;
                }
                output.Advance((int)produced);
                // Same loop condition as Reader::ReadPhysicalRecord: continue while input remains or the output
                // buffer was filled completely (zstd may hold more output).
                if (inBuf.pos >= inBuf.size && produced < (nuint)LogFormat.BlockSize) return true;
            }
        }
    }

    public void Dispose()
    {
        if (_dctx != null)
        {
            Methods.ZSTD_freeDCtx(_dctx);
            _dctx = null;
        }
        GC.SuppressFinalize(this);
    }

    ~WalZstdDecompressor() => Dispose();
}
