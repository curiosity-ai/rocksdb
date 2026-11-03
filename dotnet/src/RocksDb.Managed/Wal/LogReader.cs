using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Wal;

/// <summary>
/// Receives notifications about data dropped by a <see cref="LogReader"/>. Port of <c>log::Reader::Reporter</c>
/// (db/log_reader.h).
/// </summary>
public abstract class LogReporter
{
    /// <summary>Some corruption was detected. <paramref name="bytes"/> is the approximate number of bytes dropped.</summary>
    /// <param name="bytes">Approximate number of bytes dropped.</param>
    /// <param name="code">Status code (Corruption, or IOError for read failures).</param>
    /// <param name="reason">Reason, e.g. "checksum mismatch" (without the "Corruption: " prefix).</param>
    /// <param name="logNumber">Log number the corruption refers to, or <see cref="UnspecifiedLogNumber"/> if unspecified.</param>
    public abstract void Corruption(long bytes, StatusCode code, string reason, ulong logNumber);

    /// <summary>A record from a previous user of a recycled log was found (point-in-time recovery).</summary>
    public virtual void OldLogRecord(long bytes) { }

    /// <summary>The "unspecified" log number passed to <see cref="Corruption"/> (kMaxSequenceNumber).</summary>
    public const ulong UnspecifiedLogNumber = Db.DbFormat.MaxSequenceNumber;
}

/// <summary>
/// Reads logical records from a log file (WAL or MANIFEST). Port of <c>log::Reader</c> (db/log_reader.{h,cc}).
/// <para>
/// The reader owns a pooled 32KiB block buffer and a pooled reassembly buffer, so reading records does not allocate
/// in steady state. The span returned by <see cref="TryReadRecord(out ReadOnlySpan{byte})"/> (and
/// <see cref="Record"/>) is only valid until the next read call.
/// </para>
/// <para>
/// Corruption is reported to the <see cref="LogReporter"/> according to <see cref="RecoveryMode"/> (exactly as the
/// C++ reader does); reading continues after reported corruption where the C++ reader would.
/// I/O errors thrown by the file (<see cref="IOException"/> or an IOError <see cref="RocksDbException"/>) are reported
/// as dropped data and end the stream. Not thread-safe.
/// </para>
/// </summary>
public sealed class LogReader : IDisposable
{
    // Extend record types with the following special values (log::Reader anonymous enum).
    private const int Eof = LogFormat.MaxRecordType + 1;
    // Returned whenever we find an invalid physical record (bad CRC is reported separately; 0-length records are not).
    private const int BadRecord = LogFormat.MaxRecordType + 2;
    // Returned when we fail to read a valid header.
    private const int BadHeader = LogFormat.MaxRecordType + 3;
    // Returned when we read an old record from a previous user of the log.
    private const int OldRecord = LogFormat.MaxRecordType + 4;
    // Returned when we get a bad record length.
    private const int BadRecordLen = LogFormat.MaxRecordType + 5;
    // Returned when we get a bad record checksum.
    private const int BadRecordChecksum = LogFormat.MaxRecordType + 6;
    // Managed only: the block buffer must be refilled from the file before parsing can continue.
    private const int NeedRead = 0x1000;

    // Results of ReadRecordSteps.
    private const int StepEof = 0;
    private const int StepRecord = 1;

    private readonly ISequentialFile _file;
    private readonly LogReporter? _reporter;
    private readonly bool _checksum;
    private byte[] _backingStore;
    // buffer_: a window [_bufStart, _bufStart + _bufLen) into _backingStore.
    private int _bufStart;
    private int _bufLen;
    private bool _eof;         // Last Read() indicated EOF by returning < kBlockSize
    private bool _readError;   // Error occurred while reading from file
    // Offset of the file position indicator within the last block when an EOF was detected.
    private int _eofOffset;
    // Offset of the last record returned by ReadRecord.
    private long _lastRecordOffset;
    // Offset of the first location past the end of buffer_.
    private long _endOfBufferOffset;
    private readonly ulong _logNumber;
    // Whether this is a recycled log file
    private bool _recycled;
    // Whether the first record has been read or not.
    private bool _firstRecordRead;
    private CompressionType _compressionType;
    private bool _compressionTypeRecordRead;
    private WalZstdDecompressor? _uncompress;
    private PooledBufferWriter? _uncompressedRecord;
    private Dictionary<uint, int>? _recordedCfToTsSize;

    private PooledBufferWriter _scratch;

    // State of the ReadRecord call in progress (locals in the C++ ReadRecord; fields here so that the loop can be
    // suspended while the block buffer is refilled asynchronously).
    private bool _inFragmentedRecord;
    private long _prospectiveRecordOffset;
    private long _physicalRecordOffset;
    private bool _resumingAfterRead;
    private int _forcedPhysicalResult;

    // Current fragment / record: [offset, offset + length) of the given array.
    private byte[] _fragArray = [];
    private int _fragOffset;
    private int _fragLength;
    private byte[] _recordArray = [];
    private int _recordOffset;
    private int _recordLength;

    /// <param name="file">Source file. Owned by the reader (disposed by <see cref="Dispose"/>).</param>
    /// <param name="reporter">Notified whenever data is dropped due to corruption; may be null.</param>
    /// <param name="checksum">Verify record checksums.</param>
    /// <param name="logNumber">Number of this log; recyclable records carrying another number are treated as old.</param>
    public LogReader(ISequentialFile file, LogReporter? reporter, bool checksum, ulong logNumber)
    {
        ArgumentNullException.ThrowIfNull(file);
        _file = file;
        _reporter = reporter;
        _checksum = checksum;
        _logNumber = logNumber;
        _backingStore = ArrayPool<byte>.Shared.Rent(LogFormat.BlockSize);
        _scratch = new PooledBufferWriter(0);
    }

    /// <summary>Recovery mode used to decide which problems are reported (defaults to kTolerateCorruptedTailRecords,
    /// the default of <c>Reader::ReadRecord</c>).</summary>
    public WalRecoveryMode RecoveryMode { get; set; } = WalRecoveryMode.TolerateCorruptedTailRecords;

    /// <summary>See <c>Options::track_and_verify_wals</c>: verify predecessor-WAL-info records.</summary>
    public bool TrackAndVerifyWals { get; init; }
    /// <summary>Whether replay has already been stopped due to corruption (skips predecessor verification).</summary>
    public bool StopReplayForCorruption { get; init; }
    /// <summary>The minimum WAL number that should still exist (for predecessor verification).</summary>
    public ulong MinWalNumberToKeep { get; init; } = ulong.MaxValue;
    /// <summary>The predecessor WAL actually observed during recovery, if any.</summary>
    public PredecessorWalInfo? ObservedPredecessorWalInfo { get; init; }
    /// <summary>Name of the file, for corruption messages (<c>file_->file_name()</c>); the log number when null.</summary>
    public string? FileName { get; init; }

    public ulong LogNumber => _logNumber;
    /// <summary>Whether the reader has encountered an EOF condition.</summary>
    public bool IsEof => _eof;
    /// <summary>Whether the reader has encountered a read error.</summary>
    public bool HasReadError => _readError;
    /// <summary>Physical offset of the last record returned. Undefined before the first record.</summary>
    public long LastRecordOffset => _lastRecordOffset;
    /// <summary>The first physical offset after the last record returned (the current position in the file).</summary>
    public long LastRecordEnd => _endOfBufferOffset - _bufLen;
    /// <summary>Number of bytes read from the file so far.</summary>
    public long ReadOffset => _endOfBufferOffset;
    /// <summary>The WAL compression type read from the SetCompressionType record (None if absent).</summary>
    public CompressionType CompressionType => _compressionType;
    /// <summary>True if the log contains only a compression type record.</summary>
    public bool IsCompressedAndEmptyFile => !_firstRecordRead && _compressionTypeRecordRead;

    /// <summary>The record returned by the last successful read. Valid until the next read call.</summary>
    public ReadOnlyMemory<byte> Record => _recordArray.AsMemory(_recordOffset, _recordLength);

    /// <summary>User-defined timestamp sizes recorded so far (column family id -> timestamp size). WAL only.</summary>
    public IReadOnlyDictionary<uint, int> RecordedTimestampSizes =>
        (IReadOnlyDictionary<uint, int>?)_recordedCfToTsSize ?? System.Collections.ObjectModel.ReadOnlyDictionary<uint, int>.Empty;

    // ---- Public read API ----------------------------------------------------------------------------------------

    /// <summary>
    /// Reads the next logical record. Returns false at the end of the input. Port of <c>Reader::ReadRecord</c>.
    /// The returned span is valid until the next call on this reader.
    /// </summary>
    public bool TryReadRecord(out ReadOnlySpan<byte> record)
    {
        StartRecord();
        int r;
        while ((r = ReadRecordSteps()) == NeedRead)
        {
            ReadBlock();
        }
        record = r == StepRecord ? _recordArray.AsSpan(_recordOffset, _recordLength) : default;
        return r == StepRecord;
    }

    /// <summary>
    /// Asynchronously reads the next logical record; on true, the record is available through <see cref="Record"/>
    /// until the next call. Completes synchronously when the record is already buffered.
    /// </summary>
    public ValueTask<bool> ReadRecordAsync(CancellationToken cancellationToken = default)
    {
        StartRecord();
        int r = ReadRecordSteps();
        if (r != NeedRead) return new ValueTask<bool>(r == StepRecord);
        return ReadRecordSlowAsync(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> ReadRecordSlowAsync(CancellationToken cancellationToken)
    {
        int r;
        do
        {
            await ReadBlockAsync(cancellationToken).ConfigureAwait(false);
            r = ReadRecordSteps();
        } while (r == NeedRead);
        return r == StepRecord;
    }

    // ---- ReadRecord state machine -------------------------------------------------------------------------------

    private void StartRecord()
    {
        _scratch.Clear();
        _recordArray = [];
        _recordOffset = 0;
        _recordLength = 0;
        _uncompress?.Reset();
        _inFragmentedRecord = false;
        _prospectiveRecordOffset = 0;
        _resumingAfterRead = false;
        _forcedPhysicalResult = 0;
    }

    private void SetRecordFromFragment()
    {
        _recordArray = _fragArray;
        _recordOffset = _fragOffset;
        _recordLength = _fragLength;
    }

    private ReadOnlySpan<byte> Fragment => _fragArray.AsSpan(_fragOffset, _fragLength);

    private bool ReportsTailCorruption =>
        RecoveryMode == WalRecoveryMode.AbsoluteConsistency || RecoveryMode == WalRecoveryMode.PointInTimeRecovery;

    /// <summary>
    /// Body of the loop in <c>Reader::ReadRecord</c>. Returns <see cref="StepRecord"/>, <see cref="StepEof"/>, or
    /// <see cref="NeedRead"/> when the block buffer must be refilled before calling again.
    /// </summary>
    private int ReadRecordSteps()
    {
        for (;;)
        {
            int dropSize = 0;
            int recordType;
            if (_forcedPhysicalResult != 0)
            {
                recordType = _forcedPhysicalResult;
                _forcedPhysicalResult = 0;
                _resumingAfterRead = false;
                _fragArray = [];
                _fragOffset = _fragLength = 0;
            }
            else
            {
                if (!_resumingAfterRead) _physicalRecordOffset = _endOfBufferOffset - _bufLen;
                recordType = ReadPhysicalRecord(ref dropSize);
                if (recordType == NeedRead)
                {
                    _resumingAfterRead = true;
                    return NeedRead;
                }
                _resumingAfterRead = false;
            }

            switch (recordType)
            {
                case (int)LogRecordType.Full:
                case (int)LogRecordType.RecyclableFull:
                    if (_inFragmentedRecord && _scratch.WrittenCount != 0)
                    {
                        // Handle bug in earlier versions of log::Writer where it could emit an empty kFirstType
                        // record at the tail end of a block followed by a kFullType or kFirstType record at the
                        // beginning of the next block.
                        ReportCorruption(_scratch.WrittenCount, "partial record without end(1)");
                    }
                    _prospectiveRecordOffset = _physicalRecordOffset;
                    _scratch.Clear();
                    SetRecordFromFragment();
                    _lastRecordOffset = _prospectiveRecordOffset;
                    _firstRecordRead = true;
                    return StepRecord;

                case (int)LogRecordType.First:
                case (int)LogRecordType.RecyclableFirst:
                    if (_inFragmentedRecord && _scratch.WrittenCount != 0)
                    {
                        ReportCorruption(_scratch.WrittenCount, "partial record without end(2)");
                    }
                    _prospectiveRecordOffset = _physicalRecordOffset;
                    _scratch.Clear();
                    _scratch.Write(Fragment);
                    _inFragmentedRecord = true;
                    break;

                case (int)LogRecordType.Middle:
                case (int)LogRecordType.RecyclableMiddle:
                    if (!_inFragmentedRecord)
                    {
                        ReportCorruption(_fragLength, "missing start of fragmented record(1)");
                    }
                    else
                    {
                        _scratch.Write(Fragment);
                    }
                    break;

                case (int)LogRecordType.Last:
                case (int)LogRecordType.RecyclableLast:
                    if (!_inFragmentedRecord)
                    {
                        ReportCorruption(_fragLength, "missing start of fragmented record(2)");
                    }
                    else
                    {
                        _scratch.Write(Fragment);
                        ReadOnlyMemory<byte> written = _scratch.WrittenMemory;
                        System.Runtime.InteropServices.MemoryMarshal.TryGetArray(written, out ArraySegment<byte> seg);
                        _recordArray = seg.Array!;
                        _recordOffset = seg.Offset;
                        _recordLength = seg.Count;
                        _lastRecordOffset = _prospectiveRecordOffset;
                        _firstRecordRead = true;
                        return StepRecord;
                    }
                    break;

                case (int)LogRecordType.SetCompressionType:
                    if (_compressionTypeRecordRead)
                    {
                        ReportCorruption(_fragLength, "read multiple SetCompressionType records");
                    }
                    if (_firstRecordRead)
                    {
                        ReportCorruption(_fragLength, "SetCompressionType not the first record");
                    }
                    _prospectiveRecordOffset = _physicalRecordOffset;
                    _scratch.Clear();
                    _lastRecordOffset = _prospectiveRecordOffset;
                    if (!CompressionTypeRecord.TryDecode(Fragment, out CompressionType ct))
                    {
                        ReportCorruption(_fragLength, "could not decode SetCompressionType record");
                    }
                    else
                    {
                        InitCompression(ct);
                    }
                    break;

                case (int)LogRecordType.PredecessorWalInfo:
                case (int)LogRecordType.RecyclePredecessorWalInfo:
                    _prospectiveRecordOffset = _physicalRecordOffset;
                    _scratch.Clear();
                    _lastRecordOffset = _prospectiveRecordOffset;
                    if (!PredecessorWalInfo.TryDecode(Fragment, out PredecessorWalInfo recorded))
                    {
                        ReportCorruption(_fragLength, "could not decode PredecessorWALInfoType record");
                    }
                    else
                    {
                        MaybeVerifyPredecessorWalInfo(recorded);
                    }
                    break;

                case (int)LogRecordType.UserDefinedTimestampSize:
                case (int)LogRecordType.RecyclableUserDefinedTimestampSize:
                    if (_inFragmentedRecord && _scratch.WrittenCount != 0)
                    {
                        ReportCorruption(_scratch.WrittenCount, "user-defined timestamp size record interspersed partial record");
                    }
                    _prospectiveRecordOffset = _physicalRecordOffset;
                    _scratch.Clear();
                    _lastRecordOffset = _prospectiveRecordOffset;
                    if (!UserDefinedTimestampSizeRecord.IsValid(Fragment))
                    {
                        ReportCorruption(_fragLength, "could not decode user-defined timestamp size record");
                    }
                    else
                    {
                        string? error = UpdateRecordedTimestampSize(Fragment);
                        if (error is not null) ReportCorruption(_fragLength, error);
                    }
                    break;

                case BadHeader:
                    if (ReportsTailCorruption)
                    {
                        // In clean shutdown we don't expect any error in the log files. In point-in-time recovery an
                        // incomplete record at the end could produce a hole in the recovered data. Report an error
                        // here, which higher layers can choose to ignore when it's provable there is no hole.
                        ReportCorruption(dropSize, "truncated header");
                    }
                    goto case Eof;

                case Eof:
                    if (_inFragmentedRecord)
                    {
                        if (ReportsTailCorruption)
                        {
                            ReportCorruption(_scratch.WrittenCount, "error reading trailing data due to encountering EOF");
                        }
                        // This can be caused by the writer dying immediately after writing a physical record but
                        // before completing the next; don't treat it as a corruption, just ignore the entire
                        // logical record.
                        _scratch.Clear();
                    }
                    return StepEof;

                case OldRecord:
                    if (RecoveryMode != WalRecoveryMode.SkipAnyCorruptedRecords)
                    {
                        // Treat a record from a previous instance of the log as EOF.
                        if (_inFragmentedRecord)
                        {
                            if (ReportsTailCorruption)
                            {
                                ReportCorruption(_scratch.WrittenCount, "error reading trailing data due to encountering old record");
                            }
                            _scratch.Clear();
                        }
                        else if (RecoveryMode == WalRecoveryMode.PointInTimeRecovery)
                        {
                            _reporter?.OldLogRecord(_scratch.WrittenCount);
                        }
                        return StepEof;
                    }
                    goto case BadRecord;

                case BadRecord:
                    if (_inFragmentedRecord)
                    {
                        ReportCorruption(_scratch.WrittenCount, "error in middle of record");
                        _inFragmentedRecord = false;
                        _scratch.Clear();
                    }
                    break;

                case BadRecordLen:
                case BadRecordChecksum:
                    if (recordType == BadRecordLen && _eof)
                    {
                        if (ReportsTailCorruption)
                        {
                            ReportCorruption(dropSize, "truncated record body");
                        }
                        return StepEof;
                    }
                    if (_recycled && RecoveryMode == WalRecoveryMode.TolerateCorruptedTailRecords)
                    {
                        _scratch.Clear();
                        return StepEof;
                    }
                    ReportCorruption(dropSize, recordType == BadRecordLen ? "bad record length" : "checksum mismatch");
                    if (_inFragmentedRecord)
                    {
                        ReportCorruption(_scratch.WrittenCount, "error in middle of record");
                        _inFragmentedRecord = false;
                        _scratch.Clear();
                    }
                    break;

                default:
                    if ((recordType & LogFormat.RecordTypeSafeIgnoreMask) == 0)
                    {
                        ReportCorruption(_fragLength + (_inFragmentedRecord ? _scratch.WrittenCount : 0),
                            "unknown record type " + recordType.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    }
                    _inFragmentedRecord = false;
                    _scratch.Clear();
                    break;
            }
        }
    }

    /// <summary>
    /// Parses the next physical record from the block buffer. Port of <c>Reader::ReadPhysicalRecord</c>, except that
    /// instead of reading more data itself it returns <see cref="NeedRead"/>; the caller refills the buffer and calls
    /// again (parsing restarts from the same position, so this is equivalent).
    /// </summary>
    private int ReadPhysicalRecord(ref int dropSize)
    {
        while (true)
        {
            // We need at least the minimum header size
            if (_bufLen < LogFormat.HeaderSize)
            {
                return ReadMore(ref dropSize);
            }

            // Parse the header
            ReadOnlySpan<byte> header = _backingStore.AsSpan(_bufStart, _bufLen);
            uint a = header[4];
            uint b = header[5];
            byte type = header[6];
            int length = (int)(a | (b << 8));
            int headerSize = LogFormat.HeaderSize;
            bool isRecyclableType = LogFormat.IsRecyclableType(type);
            if (isRecyclableType)
            {
                headerSize = LogFormat.RecyclableHeaderSize;
                if (_firstRecordRead && !_recycled)
                {
                    // A recycled log should have started with a recycled record
                    return BadRecord;
                }
                _recycled = true;
                // We need enough for the larger header
                if (_bufLen < LogFormat.RecyclableHeaderSize)
                {
                    return ReadMore(ref dropSize);
                }
            }

            if (headerSize + length > _bufLen)
            {
                dropSize = _bufLen;
                ClearBuffer();
                // If the end of the read has been reached without seeing `header_size + length` bytes of payload,
                // report a corruption. The higher layers can decide how to handle it based on the recovery mode,
                // whether this occurred at EOF, whether this is the final WAL, etc.
                return BadRecordLen;
            }

            if (isRecyclableType)
            {
                uint logNum = Coding.DecodeFixed32(header[7..]);
                if (logNum != unchecked((uint)_logNumber))
                {
                    RemovePrefix(headerSize + length);
                    return OldRecord;
                }
            }

            if (type == (byte)LogRecordType.Zero && length == 0)
            {
                // Skip zero length record without reporting any drops since such records are produced by the mmap
                // based writing code in env_posix.cc that preallocates file regions.
                ClearBuffer();
                return BadRecord;
            }

            // Check crc
            if (_checksum)
            {
                uint expectedCrc = Crc32C.Unmask(Coding.DecodeFixed32(header));
                uint actualCrc = Crc32C.Value(header.Slice(6, length + headerSize - 6));
                if (actualCrc != expectedCrc)
                {
                    // Drop the rest of the buffer since "length" itself may have been corrupted and if we trust it,
                    // we could find some fragment of a real log record that just happens to look like a valid log
                    // record.
                    dropSize = _bufLen;
                    ClearBuffer();
                    return BadRecordChecksum;
                }
            }

            int payloadOffset = _bufStart + headerSize;
            RemovePrefix(headerSize + length);

            if (_uncompress is null || LogFormat.IsMetadataType(type))
            {
                _fragArray = _backingStore;
                _fragOffset = payloadOffset;
                _fragLength = length;
                return type;
            }

            // Uncompress compressed records
            PooledBufferWriter output = _uncompressedRecord!;
            output.Clear();
            if (!_uncompress.Uncompress(_backingStore.AsSpan(payloadOffset, length), output))
            {
                ClearBuffer();
                return BadRecord;
            }
            System.Runtime.InteropServices.MemoryMarshal.TryGetArray(output.WrittenMemory, out ArraySegment<byte> seg);
            _fragArray = seg.Array ?? [];
            _fragOffset = seg.Offset;
            _fragLength = seg.Count;
            return type;
        }
    }

    /// <summary>The non-I/O part of <c>Reader::ReadMore</c>.</summary>
    private int ReadMore(ref int dropSize)
    {
        if (!_eof && !_readError)
        {
            return NeedRead;
        }
        // Note that if buffer_ is non-empty, we have a truncated header at the end of the file, which can be caused
        // by the writer crashing in the middle of writing the header. Unless explicitly requested we don't consider
        // this an error, just report EOF.
        if (_bufLen != 0)
        {
            dropSize = _bufLen;
            ClearBuffer();
            return BadHeader;
        }
        ClearBuffer();
        return Eof;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ClearBuffer()
    {
        _bufStart = 0;
        _bufLen = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RemovePrefix(int n)
    {
        _bufStart += n;
        _bufLen -= n;
    }

    // ---- File I/O ---------------------------------------------------------------------------------------------

    /// <summary>I/O part of <c>Reader::ReadMore</c>: the last read was a full read, so the rest is a trailer to skip.</summary>
    private void ReadBlock()
    {
        ClearBuffer();
        int n;
        try
        {
            n = ReadFully(_backingStore.AsSpan(0, LogFormat.BlockSize));
        }
        catch (Exception ex) when (IsIOError(ex))
        {
            OnReadBlockError(ex);
            return;
        }
        OnReadBlockDone(n);
    }

    private async ValueTask ReadBlockAsync(CancellationToken cancellationToken)
    {
        ClearBuffer();
        int n;
        try
        {
            n = Math.Max(0, await _file.ReadAsync(_backingStore.AsMemory(0, LogFormat.BlockSize), cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex) when (IsIOError(ex))
        {
            OnReadBlockError(ex);
            return;
        }
        OnReadBlockDone(n);
    }

    private void OnReadBlockDone(int n)
    {
        _endOfBufferOffset += n;
        _bufStart = 0;
        _bufLen = n;
        if (n < LogFormat.BlockSize)
        {
            _eof = true;
            _eofOffset = n;
        }
    }

    private void OnReadBlockError(Exception ex)
    {
        ClearBuffer();
        ReportDrop(LogFormat.BlockSize, StatusCode.IOError, ex.Message, LogReporter.UnspecifiedLogNumber);
        _readError = true;
        _forcedPhysicalResult = Eof;
    }

    // Like SequentialFileReader::Read, a short read signals EOF (the file is not polled again until UnmarkEof).
    private int ReadFully(Span<byte> dst) => Math.Max(0, _file.Read(dst));

    private static bool IsIOError(Exception ex) =>
        ex is IOException || (ex is RocksDbException rex && rex.Code == StatusCode.IOError);

    /// <summary>
    /// When more data has been written to the file, forces the reader to look again. Also aligns the file position to
    /// the start of the next block by reading the rest of the partially read block. Port of <c>Reader::UnmarkEOF</c>.
    /// </summary>
    public void UnmarkEof()
    {
        if (_readError) return;
        _eof = false;
        if (_eofOffset == 0) return;
        int remaining = PrepareUnmarkEof();
        int added;
        try
        {
            added = ReadFully(_backingStore.AsSpan(_eofOffset, remaining));
        }
        catch (Exception ex) when (IsIOError(ex))
        {
            _readError = true;
            return;
        }
        FinishUnmarkEof(added, remaining);
    }

    /// <summary>Asynchronous <see cref="UnmarkEof"/>.</summary>
    public async ValueTask UnmarkEofAsync(CancellationToken cancellationToken = default)
    {
        if (_readError) return;
        _eof = false;
        if (_eofOffset == 0) return;
        int remaining = PrepareUnmarkEof();
        int added = 0;
        try
        {
            added = Math.Max(0, await _file.ReadAsync(_backingStore.AsMemory(_eofOffset, remaining), cancellationToken).ConfigureAwait(false));
        }
        catch (Exception ex) when (IsIOError(ex))
        {
            // Data read before the failure is lost with the exception; C++ reports it via ReportDrop.
            _readError = true;
            return;
        }
        FinishUnmarkEof(added, remaining);
    }

    // If the EOF was in the middle of a block (a partial block was read) we have to read the rest of the block as
    // ReadPhysicalRecord can only read full blocks and expects the file position indicator to be aligned to the start
    // of a block:  consumed_bytes + buffer_size() + remaining == kBlockSize
    private int PrepareUnmarkEof()
    {
        int consumed = _eofOffset - _bufLen;
        int remaining = LogFormat.BlockSize - _eofOffset;
        // backing_store_ is used to concatenate what is left in buffer_ and the remainder of the block.
        if (_bufStart != consumed)
        {
            _backingStore.AsSpan(_bufStart, _bufLen).CopyTo(_backingStore.AsSpan(consumed));
            _bufStart = consumed;
        }
        return remaining;
    }

    private void FinishUnmarkEof(int added, int remaining)
    {
        _endOfBufferOffset += added;
        int consumed = _eofOffset - _bufLen;
        _bufStart = consumed;
        _bufLen = _eofOffset + added - consumed;
        if (added < remaining)
        {
            _eof = true;
            _eofOffset += added;
        }
        else
        {
            _eofOffset = 0;
        }
    }

    // ---- Metadata records -------------------------------------------------------------------------------------

    private void InitCompression(CompressionType type)
    {
        _compressionType = type;
        _compressionTypeRecordRead = true;
        if (type == CompressionType.None) return;
        _uncompress ??= new WalZstdDecompressor();
        _uncompressedRecord ??= new PooledBufferWriter(LogFormat.BlockSize);
    }

    private string? UpdateRecordedTimestampSize(ReadOnlySpan<byte> payload)
    {
        int n = UserDefinedTimestampSizeRecord.EntryCount(payload);
        for (int i = 0; i < n; i++)
        {
            UserDefinedTimestampSizeRecord.DecodeEntry(payload, i, out uint cf, out ushort tsSize);
            // Zero user-defined timestamp size are not recorded.
            if (tsSize == 0)
            {
                return "User-defined timestamp size record contains zero timestamp size.";
            }
            _recordedCfToTsSize ??= new Dictionary<uint, int>();
            // The user-defined timestamp size record for a column family should not be updated in the same log file.
            if (!_recordedCfToTsSize.TryAdd(cf, tsSize))
            {
                return "User-defined timestamp size record contains update to recorded column family.";
            }
        }
        return null;
    }

    private void MaybeVerifyPredecessorWalInfo(PredecessorWalInfo recorded)
    {
        if (!TrackAndVerifyWals || RecoveryMode == WalRecoveryMode.SkipAnyCorruptedRecords || StopReplayForCorruption)
        {
            return;
        }
        ulong recordedLogNumber = recorded.LogNumber;
        if (ObservedPredecessorWalInfo is not { } observed)
        {
            // This is the first WAL recovered thus with no predecessor WAL info has been initialized
            if (recordedLogNumber >= MinWalNumberToKeep)
            {
                ReportCorruption(_fragLength, "Missing WAL of log number " + recordedLogNumber, recordedLogNumber);
            }
        }
        else if (observed.LogNumber != recordedLogNumber)
        {
            ReportCorruption(_fragLength, "Mismatched predecessor log number of WAL file " + (FileName ?? _logNumber.ToString(System.Globalization.CultureInfo.InvariantCulture)) + " Recorded " +
                recordedLogNumber + ". Observed " + observed.LogNumber, recordedLogNumber);
        }
        else if (observed.LastSequenceRecorded != recorded.LastSequenceRecorded)
        {
            ReportCorruption(_fragLength, "Mismatched last sequence number recorded in the WAL of log number " +
                recordedLogNumber + ". Recorded " + recorded.LastSequenceRecorded + ". Observed " +
                observed.LastSequenceRecorded + ". (Last sequence number equal to 0 indicates no WAL records)", recordedLogNumber);
        }
        else if (observed.SizeBytes != recorded.SizeBytes)
        {
            ReportCorruption(_fragLength, "Mismatched size of the WAL of log number " + recordedLogNumber + ". Recorded " +
                recorded.SizeBytes + " bytes. Observed " + observed.SizeBytes + " bytes.", recordedLogNumber);
        }
    }

    private void ReportCorruption(long bytes, string reason) =>
        ReportDrop(bytes, StatusCode.Corruption, reason, LogReporter.UnspecifiedLogNumber);

    private void ReportCorruption(long bytes, string reason, ulong logNumber) =>
        ReportDrop(bytes, StatusCode.Corruption, reason, logNumber);

    private void ReportDrop(long bytes, StatusCode code, string reason, ulong logNumber) =>
        _reporter?.Corruption(bytes, code, reason, logNumber);

    /// <summary>Releases the pooled buffers and disposes the file.</summary>
    public void Dispose()
    {
        byte[] b = _backingStore;
        _backingStore = [];
        if (b.Length != 0) ArrayPool<byte>.Shared.Return(b);
        _scratch.Dispose();
        _uncompressedRecord?.Dispose();
        _uncompress?.Dispose();
        _uncompress = null;
        _file.Dispose();
    }
}
