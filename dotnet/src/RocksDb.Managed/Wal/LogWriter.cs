using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Wal;

/// <summary>
/// Appends logical records to a log file (WAL or MANIFEST), fragmenting them across 32KiB blocks. Port of
/// <c>log::Writer</c> (db/log_writer.{h,cc}); see <see cref="LogFormat"/> for the layout.
/// <para>
/// In steady state <see cref="AddRecord(ReadOnlySpan{byte})"/> and <see cref="AddRecordAsync(ReadOnlyMemory{byte}, CancellationToken)"/>
/// do not allocate: headers are formatted into a per-writer buffer and the payload is handed to
/// <see cref="IWritableFile.Append"/>, which copies it into the file's own buffer.
/// </para>
/// <para>Not thread-safe: the caller (the DB write path) serializes access, as in RocksDB.</para>
/// </summary>
public sealed class LogWriter : IDisposable, IAsyncDisposable
{
    // crc32c of each possible type byte, pre-computed to reduce the overhead of computing the crc of the record type
    // stored in the header (Writer::type_crc_).
    private static readonly uint[] s_typeCrc = CreateTypeCrcTable();
    // Zero bytes for block trailers. A trailer can be up to a whole block in MaybeSwitchToNewBlock.
    private static readonly byte[] s_zeros = new byte[LogFormat.BlockSize];

    private IWritableFile? _dest;
    private int _blockOffset; // Current offset in block
    private readonly ulong _logNumber;
    private readonly bool _recycleLogFiles;
    private readonly int _headerSize;
    private readonly bool _manualFlush;
    private readonly byte[] _header = new byte[LogFormat.RecyclableHeaderSize];

    private CompressionType _compressionType;
    private WalZstdCompressor? _compress;
    private byte[]? _compressedBuffer;

    // Column families whose user-defined timestamp size has already been recorded in this log.
    private Dictionary<uint, int>? _recordedCfToTsSize;

    /// <summary>Creates a writer with no WAL compression, auto flush after each record, starting at block offset 0.</summary>
    /// <param name="dest">File to append to. Owned by the writer (disposed by <see cref="Dispose"/>).</param>
    /// <param name="logNumber">Log file number; its low 32 bits are written into recyclable headers.</param>
    /// <param name="recycleLogFiles">Use the recyclable (11-byte) record format.</param>
    public LogWriter(IWritableFile dest, ulong logNumber, bool recycleLogFiles)
        : this(dest, logNumber, recycleLogFiles, manualFlush: false, CompressionType.None, initialBlockOffset: 0)
    {
    }

    /// <param name="dest">File to append to. Owned by the writer.</param>
    /// <param name="logNumber">Log file number; its low 32 bits are written into recyclable headers.</param>
    /// <param name="recycleLogFiles">Use the recyclable (11-byte) record format.</param>
    /// <param name="manualFlush">When false, the file is flushed (pushed to the OS) after every record.</param>
    /// <param name="compressionType">WAL compression (None or Zstd). Takes effect after <see cref="AddCompressionTypeRecord"/>.</param>
    /// <param name="initialBlockOffset">Offset within the current block, when appending to an existing log.</param>
    public LogWriter(IWritableFile dest, ulong logNumber, bool recycleLogFiles, bool manualFlush,
        CompressionType compressionType, int initialBlockOffset)
    {
        ArgumentNullException.ThrowIfNull(dest);
        ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)initialBlockOffset, (uint)LogFormat.BlockSize);
        if (compressionType != CompressionType.None && compressionType != CompressionType.Zstd)
            ThrowHelper.NotSupported("WAL compression type " + compressionType);
        _dest = dest;
        _logNumber = logNumber;
        _recycleLogFiles = recycleLogFiles;
        _headerSize = recycleLogFiles ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;
        _manualFlush = manualFlush;
        _compressionType = compressionType;
        _blockOffset = initialBlockOffset;
    }

    public ulong LogNumber => _logNumber;
    public bool IsRecyclable => _recycleLogFiles;
    /// <summary>Offset of the next physical record within the current block (TEST_block_offset).</summary>
    public int BlockOffset => _blockOffset;
    /// <summary>The largest sequence number passed to <c>AddRecord</c> so far (for predecessor WAL info).</summary>
    public ulong LastSequenceRecorded { get; private set; }
    /// <summary>The compression type that records are written with (None until the compression record is written).</summary>
    public CompressionType CompressionType => _compress is null ? CompressionType.None : _compressionType;
    /// <summary>The underlying file (null after close).</summary>
    public IWritableFile? File => _dest;

    private static uint[] CreateTypeCrcTable()
    {
        var t = new uint[LogFormat.MaxRecordType + 1];
        Span<byte> b = stackalloc byte[1];
        for (int i = 0; i < t.Length; i++)
        {
            b[0] = (byte)i;
            t[i] = Crc32C.Value(b);
        }
        return t;
    }

    private IWritableFile Dest
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            IWritableFile? d = _dest;
            if (d is null) ThrowClosed();
            return d;
        }
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void ThrowClosed() => throw new ObjectDisposedException(nameof(LogWriter));

    // ---- AddRecord ----------------------------------------------------------------------------------------------

    private struct AddState
    {
        public int Left;           // bytes of the current payload source not yet emitted
        public int Pos;            // offset into the current payload source
        public int CompressRemaining;
        public bool Begin;
        public bool CompressStart;
        public bool FromCompressed; // payload source is _compressedBuffer rather than the record
    }

    private AddState BeginRecord(int length)
    {
        var st = new AddState { Left = length, Begin = true };
        if (_compress is not null)
        {
            _compress.Reset();
            st.CompressStart = true;
        }
        return st;
    }

    /// <summary>If the current block cannot hold another header, returns the trailer length to zero-fill and starts a new block.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int TakeTrailer()
    {
        int leftover = LogFormat.BlockSize - _blockOffset;
        if (leftover < _headerSize)
        {
            _blockOffset = 0; // Switch to a new block
            return leftover;
        }
        return 0;
    }

    /// <summary>
    /// Plans the next fragment of the record and formats its header into <see cref="_header"/>. Returns false when
    /// there is nothing more to emit (only possible with compression). Port of the body of the loop in
    /// <c>Writer::AddRecord</c>.
    /// </summary>
    private bool PrepareFragment(ReadOnlySpan<byte> record, ref AddState st, out int headerSize, out int fragmentLength)
    {
        // Invariant: we never leave < header_size bytes in a block.
        int avail = LogFormat.BlockSize - _blockOffset - _headerSize;

        if (_compress is not null && (st.CompressStart || st.Left == 0))
        {
            _compressedBuffer ??= new byte[LogFormat.BlockSize - _headerSize];
            st.CompressRemaining = _compress.Compress(record, _compressedBuffer, out int written);
            if (st.CompressRemaining < 0)
            {
                throw RocksDbException.IOError("Unexpected WAL compression error");
            }
            st.Left = written;
            st.Pos = 0;
            if (written == 0 && !st.CompressStart)
            {
                headerSize = 0;
                fragmentLength = 0;
                return false; // Nothing left to compress
            }
            st.CompressStart = false;
            st.FromCompressed = true;
        }

        fragmentLength = st.Left < avail ? st.Left : avail;
        bool end = st.Left == fragmentLength && st.CompressRemaining == 0;
        LogRecordType type;
        if (st.Begin && end) type = _recycleLogFiles ? LogRecordType.RecyclableFull : LogRecordType.Full;
        else if (st.Begin) type = _recycleLogFiles ? LogRecordType.RecyclableFirst : LogRecordType.First;
        else if (end) type = _recycleLogFiles ? LogRecordType.RecyclableLast : LogRecordType.Last;
        else type = _recycleLogFiles ? LogRecordType.RecyclableMiddle : LogRecordType.Middle;

        ReadOnlySpan<byte> payload = st.FromCompressed
            ? _compressedBuffer.AsSpan(st.Pos, fragmentLength)
            : record.Slice(st.Pos, fragmentLength);
        headerSize = FormatHeader(type, payload);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AdvanceFragment(ref AddState st, int fragmentLength)
    {
        st.Pos += fragmentLength;
        st.Left -= fragmentLength;
        st.Begin = false;
    }

    /// <summary>
    /// Formats the physical record header for <paramref name="payload"/> into <see cref="_header"/> and advances the
    /// block offset. Returns the header size. Port of the header part of <c>Writer::EmitPhysicalRecord</c>.
    /// </summary>
    private int FormatHeader(LogRecordType t, ReadOnlySpan<byte> payload)
    {
        int n = payload.Length;
        System.Diagnostics.Debug.Assert(n <= 0xffff); // Must fit in two bytes
        byte[] buf = _header;
        buf[4] = (byte)(n & 0xff);
        buf[5] = (byte)(n >> 8);
        buf[6] = (byte)t;

        uint crc = s_typeCrc[(byte)t];
        int headerSize;
        if (!LogFormat.IsRecyclableType((byte)t))
        {
            // Legacy record format
            System.Diagnostics.Debug.Assert(_blockOffset + LogFormat.HeaderSize + n <= LogFormat.BlockSize);
            headerSize = LogFormat.HeaderSize;
        }
        else
        {
            // Recyclable record format
            System.Diagnostics.Debug.Assert(_blockOffset + LogFormat.RecyclableHeaderSize + n <= LogFormat.BlockSize);
            headerSize = LogFormat.RecyclableHeaderSize;
            // Only encode low 32-bits of the 64-bit log number. This means we will fail to detect an old record if
            // we recycled a log from ~4 billion logs ago, but that is effectively impossible.
            Coding.EncodeFixed32(buf.AsSpan(7), unchecked((uint)_logNumber));
            crc = Crc32C.Extend(crc, buf.AsSpan(7, 4));
        }

        // Compute the crc of the record type and the payload.
        crc = Crc32C.Extend(crc, payload);
        Coding.EncodeFixed32(buf, Crc32C.Mask(crc)); // Adjust for storage
        _blockOffset += headerSize + n;
        return headerSize;
    }

    /// <summary>Appends a logical record. Port of <c>Writer::AddRecord</c>.</summary>
    public void AddRecord(ReadOnlySpan<byte> record) => AddRecord(record, 0);

    /// <summary>Appends a logical record that contains updates up to sequence <paramref name="sequence"/>.</summary>
    public void AddRecord(ReadOnlySpan<byte> record, ulong sequence)
    {
        ThrowIfSeenError();
        try
        {
            AddRecordCore(record, sequence);
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    private void AddRecordCore(ReadOnlySpan<byte> record, ulong sequence)
    {
        IWritableFile dest = Dest;
        // Fragment the record if necessary and emit it. Note that if record is empty, we still want to iterate once
        // to emit a single zero-length record.
        AddState st = BeginRecord(record.Length);
        do
        {
            int trailer = TakeTrailer();
            if (trailer > 0) dest.Append(s_zeros.AsSpan(0, trailer)); // Fill the trailer
            if (!PrepareFragment(record, ref st, out int headerSize, out int fragmentLength)) break;
            dest.Append(_header.AsSpan(0, headerSize));
            dest.Append(st.FromCompressed ? _compressedBuffer.AsSpan(st.Pos, fragmentLength) : record.Slice(st.Pos, fragmentLength));
            AdvanceFragment(ref st, fragmentLength);
        } while (st.Left > 0 || st.CompressRemaining > 0);

        if (!_manualFlush) dest.Flush();
        if (sequence > LastSequenceRecorded) LastSequenceRecorded = sequence;
    }

    /// <summary>Asynchronously appends a logical record. Port of <c>Writer::AddRecord</c>.</summary>
    public ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken = default) =>
        AddRecordAsync(record, 0, cancellationToken);

    /// <summary>Asynchronously appends a logical record that contains updates up to sequence <paramref name="sequence"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, ulong sequence, CancellationToken cancellationToken = default)
    {
        ThrowIfSeenError();
        try
        {
            IWritableFile dest = Dest;
            AddState st = BeginRecord(record.Length);
            do
            {
                int trailer = TakeTrailer();
                if (trailer > 0) await dest.AppendAsync(s_zeros.AsMemory(0, trailer), cancellationToken).ConfigureAwait(false);
                if (!PrepareFragment(record.Span, ref st, out int headerSize, out int fragmentLength)) break;
                await dest.AppendAsync(_header.AsMemory(0, headerSize), cancellationToken).ConfigureAwait(false);
                ReadOnlyMemory<byte> payload = st.FromCompressed
                    ? _compressedBuffer.AsMemory(st.Pos, fragmentLength)
                    : record.Slice(st.Pos, fragmentLength);
                await dest.AppendAsync(payload, cancellationToken).ConfigureAwait(false);
                AdvanceFragment(ref st, fragmentLength);
            } while (st.Left > 0 || st.CompressRemaining > 0);

            if (!_manualFlush) await dest.FlushAsync(cancellationToken).ConfigureAwait(false);
            if (sequence > LastSequenceRecorded) LastSequenceRecorded = sequence;
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    // ---- Metadata records ---------------------------------------------------------------------------------------

    /// <summary>
    /// Writes the <see cref="LogRecordType.SetCompressionType"/> record (must be the first record) and enables
    /// compression for subsequent records. No-op when the writer's compression type is None.
    /// Port of <c>Writer::AddCompressionTypeRecord</c>.
    /// </summary>
    public void AddCompressionTypeRecord()
    {
        if (_compressionType == CompressionType.None) return; // No need to add a record
        if (_blockOffset != 0) ThrowHelper.InvalidArgument("The compression type record must be the first record of the log");
        IWritableFile dest = Dest;
        Span<byte> payload = stackalloc byte[CompressionTypeRecord.EncodedLength];
        CompressionTypeRecord.EncodeTo(payload, _compressionType);
        try
        {
            EmitPhysicalRecord(dest, LogRecordType.SetCompressionType, payload);
            if (!_manualFlush) dest.Flush();
        }
        catch
        {
            _compressionType = CompressionType.None; // Disable compression if the record could not be added.
            throw;
        }
        _compress = new WalZstdCompressor();
    }

    /// <summary>
    /// Writes a predecessor-WAL-info record (used with <c>track_and_verify_wals</c>). Port of
    /// <c>Writer::MaybeAddPredecessorWALInfo</c> (the caller decides whether tracking is enabled).
    /// </summary>
    public void AddPredecessorWalInfo(PredecessorWalInfo info)
    {
        IWritableFile dest = Dest;
        Span<byte> payload = stackalloc byte[PredecessorWalInfo.EncodedLength];
        info.EncodeTo(payload);
        MaybeSwitchToNewBlock(dest, payload.Length);
        EmitPhysicalRecord(dest, _recycleLogFiles ? LogRecordType.RecyclePredecessorWalInfo : LogRecordType.PredecessorWalInfo, payload);
        if (!_manualFlush) dest.Flush();
    }

    /// <summary>
    /// Records the user-defined timestamp sizes of column families that have not been recorded in this log yet
    /// (zero sizes are not recorded). Port of <c>Writer::MaybeAddUserDefinedTimestampSizeRecord</c>.
    /// </summary>
    public void MaybeAddUserDefinedTimestampSizeRecord(KeyValuePair<uint, int>[] cfToTimestampSize)
    {
        // Allocation-free fast path for the per-write call: every column family already recorded in this log.
        Dictionary<uint, int>? recorded = _recordedCfToTsSize;
        if (recorded is not null && recorded.Count >= cfToTimestampSize.Length)
        {
            bool all = true;
            foreach (KeyValuePair<uint, int> kv in cfToTimestampSize)
            {
                if (kv.Value != 0 && !recorded.ContainsKey(kv.Key))
                {
                    all = false;
                    break;
                }
            }
            if (all) return;
        }
        MaybeAddUserDefinedTimestampSizeRecord((IEnumerable<KeyValuePair<uint, int>>)cfToTimestampSize);
    }

    public void MaybeAddUserDefinedTimestampSizeRecord(IEnumerable<KeyValuePair<uint, int>> cfToTimestampSize)
    {
        ArgumentNullException.ThrowIfNull(cfToTimestampSize);
        List<KeyValuePair<uint, int>>? toRecord = null;
        foreach (KeyValuePair<uint, int> kv in cfToTimestampSize)
        {
            _recordedCfToTsSize ??= new Dictionary<uint, int>();
            if (_recordedCfToTsSize.TryGetValue(kv.Key, out int recorded))
            {
                // A column family's user-defined timestamp size should not be updated while DB is running.
                System.Diagnostics.Debug.Assert(recorded == kv.Value);
            }
            else if (kv.Value != 0)
            {
                if ((uint)kv.Value > ushort.MaxValue) ThrowHelper.InvalidArgument("timestamp size too large");
                (toRecord ??= new List<KeyValuePair<uint, int>>()).Add(kv);
                _recordedCfToTsSize.Add(kv.Key, kv.Value);
            }
        }
        if (toRecord is null) return;

        IWritableFile dest = Dest;
        byte[] payload = new byte[UserDefinedTimestampSizeRecord.EncodedLength(toRecord.Count)];
        for (int i = 0; i < toRecord.Count; i++)
        {
            UserDefinedTimestampSizeRecord.EncodeEntry(payload.AsSpan(i * UserDefinedTimestampSizeRecord.SizePerColumnFamily),
                toRecord[i].Key, (ushort)toRecord[i].Value);
        }
        MaybeSwitchToNewBlock(dest, payload.Length);
        EmitPhysicalRecord(dest, _recycleLogFiles ? LogRecordType.RecyclableUserDefinedTimestampSize : LogRecordType.UserDefinedTimestampSize, payload);
    }

    private void EmitPhysicalRecord(IWritableFile dest, LogRecordType type, ReadOnlySpan<byte> payload)
    {
        int headerSize = FormatHeader(type, payload);
        dest.Append(_header.AsSpan(0, headerSize));
        dest.Append(payload);
    }

    /// <summary>If there's not enough space in the block for a header plus <paramref name="contentLength"/>, pads to a new block.</summary>
    private void MaybeSwitchToNewBlock(IWritableFile dest, int contentLength)
    {
        int leftover = LogFormat.BlockSize - _blockOffset;
        if (leftover < _headerSize + contentLength)
        {
            if (leftover > 0) dest.Append(s_zeros.AsSpan(0, leftover));
            _blockOffset = 0;
        }
    }

    // ---- Flush / sync / close -----------------------------------------------------------------------------------

    // WritableFileWriter::seen_error_: set by a failed record append or sync. The file's contents and durability are
    // unknown from then on, so later appends and syncs fail ("Writer has previous error.") instead of reaching the file:
    // an fsync retried after a failure can report success although the data was dropped, and the WAL must never be
    // treated as synced then. Written and read by the WAL's single writer (write leader / WAL write lock).
    private bool _seenError;

    /// <summary>A record append or a sync of this WAL failed (WritableFileWriter::seen_error).</summary>
    public bool SeenError => _seenError;

    /// <summary>WritableFileWriter::reset_seen_error: makes the file writable again (DBImpl::WALIOStatusCheck).</summary>
    public void ResetSeenError() => _seenError = false;

    private void ThrowIfSeenError()
    {
        if (_seenError) throw new RocksDbException(StatusCode.IOError, "IO error: Writer has previous error.") { WriterHasPreviousError = true };
    }

    /// <summary>Pushes buffered data to the OS (<c>Writer::WriteBuffer</c>).</summary>
    public void WriteBuffer() => Dest.Flush();
    public ValueTask WriteBufferAsync(CancellationToken cancellationToken = default) => Dest.FlushAsync(cancellationToken);

    /// <summary>Flushes and fsyncs the log file.</summary>
    public void Sync()
    {
        ThrowIfSeenError();
        try
        {
            Dest.Sync();
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfSeenError();
        try
        {
            await Dest.SyncAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    /// <summary>Closes (flushes and disposes) the underlying file.</summary>
    public void Dispose()
    {
        IWritableFile? d = _dest;
        _dest = null;
        d?.Dispose();
        _compress?.Dispose();
        _compress = null;
    }

    public async ValueTask DisposeAsync()
    {
        IWritableFile? d = _dest;
        _dest = null;
        if (d is not null) await d.DisposeAsync().ConfigureAwait(false);
        _compress?.Dispose();
        _compress = null;
    }
}
