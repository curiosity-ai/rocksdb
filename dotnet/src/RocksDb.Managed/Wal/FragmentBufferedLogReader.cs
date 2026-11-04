using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Wal;

/// <summary>
/// A log reader for files that are still being appended to (WAL tailing). Port of
/// <c>log::FragmentBufferedReader</c> (db/log_reader.{h,cc}), used by the transaction log iterator.
/// <para>
/// Unlike <see cref="LogReader"/>, reaching the end of the available data in the middle of a physical or logical
/// record is not an error: the partial header / payload stays buffered and the fragments of a partially written
/// logical record are kept, so a later read (after the writer appended more) continues where this one stopped.
/// <see cref="TryReadRecord(out ReadOnlySpan{byte})"/> returning false therefore means "nothing complete yet", not
/// "end of log"; the reader re-polls the file on every call (TryReadMore + UnmarkEOF).
/// </para>
/// <para>
/// Owns a pooled 32KiB block buffer and a pooled fragment buffer; steady-state reads do not allocate. The returned
/// record is valid until the next read call. Not thread-safe.
/// </para>
/// </summary>
public sealed class FragmentBufferedLogReader : IDisposable
{
    // log::Reader extended record types (kEof .. kBadRecordChecksum).
    private const int Eof = LogFormat.MaxRecordType + 1;
    private const int BadRecord = LogFormat.MaxRecordType + 2;
    private const int BadHeader = LogFormat.MaxRecordType + 3;
    private const int OldRecord = LogFormat.MaxRecordType + 4;
    private const int BadRecordLen = LogFormat.MaxRecordType + 5;
    private const int BadRecordChecksum = LogFormat.MaxRecordType + 6;

    // Results of the I/O-free part of TryReadFragment.
    private const int FragmentReady = 0;
    private const int NeedMore = 1;

    private ISequentialFile _file;
    private readonly LogReporter? _reporter;
    private readonly bool _checksum;
    private readonly ulong _logNumber;
    private byte[] _backingStore;
    private int _bufStart;
    private int _bufLen;
    private bool _eof;
    private bool _readError;
    private int _eofOffset;
    private long _lastRecordOffset;
    private long _endOfBufferOffset;
    private bool _recycled;
    private bool _firstRecordRead;
    private bool _compressionTypeRecordRead;
    private CompressionType _compressionType;
    private WalZstdDecompressor? _uncompress;
    private PooledBufferWriter? _uncompressedRecord;
    private bool _inFragmentedRecord;
    private PooledBufferWriter _fragments;
    // Recycled log tailing: the file offset of the first byte that may be stale (a record of the file's previous use, or
    // one the writer is overwriting right now), -1 when none. See AtStaleTail / Reposition.
    private long _staleResumeOffset = -1;
    // File offset of the First fragment of a logical record not completed yet (-1 = none), and the offset right after
    // the last record (or metadata record) read completely.
    private long _openRecordStart = -1;
    private long _consumedOffset;
    // Bytes of the next block read to discard (Reposition to an offset inside a block).
    private int _pendingDiscard;

    // Current fragment / record.
    private byte[] _fragArray = [];
    private int _fragOffset;
    private int _fragLength;
    private int _fragType;
    private int _dropSize;
    private byte[] _recordArray = [];
    private int _recordOffset;
    private int _recordLength;

    /// <param name="file">Source file (owned; disposed with the reader).</param>
    /// <param name="reporter">Notified of dropped data; may be null.</param>
    /// <param name="checksum">Verify record checksums.</param>
    /// <param name="logNumber">The log number (recyclable records carrying another number are old records).</param>
    public FragmentBufferedLogReader(ISequentialFile file, LogReporter? reporter, bool checksum, ulong logNumber)
    {
        ArgumentNullException.ThrowIfNull(file);
        _file = file;
        _reporter = reporter;
        _checksum = checksum;
        _logNumber = logNumber;
        _backingStore = ArrayPool<byte>.Shared.Rent(LogFormat.BlockSize);
        _fragments = new PooledBufferWriter(0);
    }

    public ulong LogNumber => _logNumber;
    /// <summary>The reader hit the current end of the file (more data may be appended later).</summary>
    public bool IsEof => _eof;
    public bool HasReadError => _readError;
    public long LastRecordOffset => _lastRecordOffset;
    public CompressionType CompressionType => _compressionType;
    /// <summary>
    /// The last read of a recycled log returned no record, so the bytes after the last complete record may be stale (a
    /// record of the file's previous use, a checksum mismatch, or a partial record). A recycled WAL is overwritten in
    /// place, so those bytes can be replaced by new records while the reader tails it, but the reader has already
    /// buffered (or dropped) them. <see cref="Reposition"/> re-reads the file from <see cref="StaleResumeOffset"/>.
    /// Never set for a log that is not recycled.
    /// </summary>
    public bool AtStaleTail => _staleResumeOffset >= 0;

    /// <summary>File offset to continue from after <see cref="AtStaleTail"/> (the start of the record that was stale or cut).</summary>
    public long StaleResumeOffset => _staleResumeOffset;

    /// <summary>
    /// Continues reading from <see cref="StaleResumeOffset"/> on <paramref name="file"/>, a fresh handle of the same log
    /// opened at offset 0 (this reader's handle is disposed). The recyclable and compression state is kept. Requires
    /// <see cref="AtStaleTail"/>.
    /// </summary>
    public void Reposition(ISequentialFile file)
    {
        ArgumentNullException.ThrowIfNull(file);
        if (_staleResumeOffset < 0) throw new InvalidOperationException("the reader is not at a stale tail");
        long offset = _staleResumeOffset;
        long blockStart = offset - offset % LogFormat.BlockSize;
        if (blockStart > 0) file.Skip(blockStart);
        ISequentialFile previous = _file;
        _file = file;
        previous.Dispose();
        _bufStart = _bufLen = 0;
        _eof = false;
        _eofOffset = 0;
        _readError = false;
        _endOfBufferOffset = blockStart;
        _pendingDiscard = (int)(offset - blockStart);
        _inFragmentedRecord = false;
        _fragments.Clear();
        _pendingFragmentsClear = false;
        _staleResumeOffset = -1;
        _openRecordStart = -1;
        _consumedOffset = offset;
    }

    /// <summary>
    /// A read returned no record. In a recycled log whatever follows the last complete record may be stale (the reader
    /// cannot tell a record of the previous use, or bytes cut by the writer overwriting them, from the end of the new
    /// data), so remember where to re-read from.
    /// </summary>
    private bool NoRecord()
    {
        _staleResumeOffset = _recycled ? (_openRecordStart >= 0 ? _openRecordStart : _consumedOffset) : -1;
        return false;
    }

    /// <summary>Notes that a record (or metadata record) ended at the current position.</summary>
    private void RecordConsumed()
    {
        _openRecordStart = -1;
        _consumedOffset = _endOfBufferOffset - _bufLen;
    }

    /// <summary>The record returned by the last successful read; valid until the next read call.</summary>
    public ReadOnlyMemory<byte> Record => _recordArray.AsMemory(_recordOffset, _recordLength);

    /// <summary>
    /// Reads the next complete logical record (FragmentBufferedReader::ReadRecord). Returns false when no complete
    /// record is available yet (partial data stays buffered for the next call) or after a read error.
    /// </summary>
    public bool TryReadRecord(out ReadOnlySpan<byte> record)
    {
        int r;
        StartRecord(out long physicalRecordOffset);
        while (true)
        {
            r = TryReadFragmentCore();
            if (r == NeedMore)
            {
                int oldSize = _bufLen;
                if (!TryReadMoreSync() || _bufLen == oldSize)
                {
                    record = default;
                    return NoRecord();
                }
                continue;
            }
            if (HandleFragment(ref physicalRecordOffset))
            {
                record = _recordArray.AsSpan(_recordOffset, _recordLength);
                return true;
            }
            if (_stopRead)
            {
                _stopRead = false;
                record = default;
                return NoRecord();
            }
        }
    }

    /// <summary>Asynchronous <see cref="TryReadRecord(out ReadOnlySpan{byte})"/>; the record is in <see cref="Record"/>.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> ReadRecordAsync(CancellationToken cancellationToken = default)
    {
        StartRecord(out long physicalRecordOffset);
        while (true)
        {
            int r = TryReadFragmentCore();
            if (r == NeedMore)
            {
                int oldSize = _bufLen;
                if (!await TryReadMoreAsync(cancellationToken).ConfigureAwait(false) || _bufLen == oldSize) return NoRecord();
                continue;
            }
            if (HandleFragment(ref physicalRecordOffset)) return true;
            if (_stopRead)
            {
                _stopRead = false;
                return NoRecord();
            }
        }
    }

    private void StartRecord(out long physicalRecordOffset)
    {
        _recordArray = [];
        _recordOffset = _recordLength = 0;
        _uncompress?.Reset();
        _staleResumeOffset = -1;
        physicalRecordOffset = _endOfBufferOffset - _bufLen;
    }

    // ---- ReadRecord switch ------------------------------------------------------------------------------------

    /// <summary>Processes the fragment produced by TryReadFragment; returns true when a full record is ready.</summary>
    private bool HandleFragment(ref long physicalRecordOffset)
    {
        switch (_fragType)
        {
            case (int)LogRecordType.Full:
            case (int)LogRecordType.RecyclableFull:
                if (_inFragmentedRecord && _fragments.WrittenCount != 0)
                {
                    ReportCorruption(_fragments.WrittenCount, "partial record without end(1)");
                }
                _fragments.Clear();
                _recordArray = _fragArray;
                _recordOffset = _fragOffset;
                _recordLength = _fragLength;
                _lastRecordOffset = physicalRecordOffset;
                _firstRecordRead = true;
                _inFragmentedRecord = false;
                RecordConsumed();
                return true;

            case (int)LogRecordType.First:
            case (int)LogRecordType.RecyclableFirst:
                if (_inFragmentedRecord || _fragments.WrittenCount != 0)
                {
                    ReportCorruption(_fragments.WrittenCount, "partial record without end(2)");
                }
                _fragments.Clear();
                _fragments.Write(_fragArray.AsSpan(_fragOffset, _fragLength));
                _inFragmentedRecord = true;
                _openRecordStart = _lastFragmentOffset;
                return false;

            case (int)LogRecordType.Middle:
            case (int)LogRecordType.RecyclableMiddle:
                if (!_inFragmentedRecord) ReportCorruption(_fragLength, "missing start of fragmented record(1)");
                else _fragments.Write(_fragArray.AsSpan(_fragOffset, _fragLength));
                return false;

            case (int)LogRecordType.Last:
            case (int)LogRecordType.RecyclableLast:
                if (!_inFragmentedRecord)
                {
                    ReportCorruption(_fragLength, "missing start of fragmented record(2)");
                    return false;
                }
                _fragments.Write(_fragArray.AsSpan(_fragOffset, _fragLength));
                System.Runtime.InteropServices.MemoryMarshal.TryGetArray(_fragments.WrittenMemory, out ArraySegment<byte> seg);
                _recordArray = seg.Array ?? [];
                _recordOffset = seg.Offset;
                _recordLength = seg.Count;
                // The fragments buffer becomes the record (scratch); it is cleared at the next First fragment.
                _inFragmentedRecord = false;
                _firstRecordRead = true;
                _lastRecordOffset = physicalRecordOffset;
                _pendingFragmentsClear = true;
                RecordConsumed();
                return true;

            case (int)LogRecordType.SetCompressionType:
                if (_compressionTypeRecordRead) ReportCorruption(_fragLength, "read multiple SetCompressionType records");
                if (_firstRecordRead) ReportCorruption(_fragLength, "SetCompressionType not the first record");
                _fragments.Clear();
                _lastRecordOffset = physicalRecordOffset;
                _inFragmentedRecord = false;
                RecordConsumed();
                if (!CompressionTypeRecord.TryDecode(_fragArray.AsSpan(_fragOffset, _fragLength), out CompressionType ct))
                {
                    ReportCorruption(_fragLength, "could not decode SetCompressionType record");
                }
                else
                {
                    InitCompression(ct);
                }
                return false;

            case (int)LogRecordType.PredecessorWalInfo:
            case (int)LogRecordType.RecyclePredecessorWalInfo:
                _fragments.Clear();
                _lastRecordOffset = physicalRecordOffset;
                _inFragmentedRecord = false;
                RecordConsumed();
                if (!PredecessorWalInfo.TryDecode(_fragArray.AsSpan(_fragOffset, _fragLength), out _))
                {
                    ReportCorruption(_fragLength, "could not decode PredecessorWALInfoType record");
                }
                return false;

            case (int)LogRecordType.UserDefinedTimestampSize:
            case (int)LogRecordType.RecyclableUserDefinedTimestampSize:
                if (_inFragmentedRecord && _fragments.WrittenCount != 0)
                {
                    ReportCorruption(_fragments.WrittenCount, "user-defined timestamp size record interspersed partial record");
                }
                _fragments.Clear();
                _lastRecordOffset = physicalRecordOffset;
                _inFragmentedRecord = false;
                RecordConsumed();
                if (!UserDefinedTimestampSizeRecord.IsValid(_fragArray.AsSpan(_fragOffset, _fragLength)))
                {
                    ReportCorruption(_fragLength, "could not decode user-defined timestamp size record");
                }
                return false;

            case BadHeader:
            case BadRecord:
            case Eof:
            case OldRecord:
                if (_inFragmentedRecord)
                {
                    ReportCorruption(_fragments.WrittenCount, "error in middle of record");
                    _inFragmentedRecord = false;
                    _fragments.Clear();
                }
                return false;

            case BadRecordChecksum:
                if (_recycled)
                {
                    _fragments.Clear();
                    // Like upstream: a recycled log ends at the first bad checksum; ReadRecord returns false.
                    _stopRead = true;
                    return false;
                }
                ReportCorruption(_dropSize, "checksum mismatch");
                if (_inFragmentedRecord)
                {
                    ReportCorruption(_fragments.WrittenCount, "error in middle of record");
                    _inFragmentedRecord = false;
                    _fragments.Clear();
                }
                return false;

            default:
                if ((_fragType & LogFormat.RecordTypeSafeIgnoreMask) == 0)
                {
                    ReportCorruption(_fragLength + (_inFragmentedRecord ? _fragments.WrittenCount : 0),
                        "unknown record type " + _fragType.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                _inFragmentedRecord = false;
                _fragments.Clear();
                return false;
        }
    }

    private bool _pendingFragmentsClear;
    private bool _stopRead;
    // File offset of the physical record consumed last by TryReadFragmentCore.
    private long _lastFragmentOffset;

    // ---- TryReadFragment (I/O-free part) ---------------------------------------------------------------------

    /// <summary>
    /// Port of FragmentBufferedReader::TryReadFragment without the TryReadMore calls: returns
    /// <see cref="NeedMore"/> when the buffer does not hold a complete physical record (nothing is consumed), else
    /// sets <see cref="_fragType"/> (record type or a kBad*/kOldRecord code) and the fragment.
    /// </summary>
    private int TryReadFragmentCore()
    {
        if (_pendingFragmentsClear)
        {
            // The previous record lived in the fragments buffer; it is no longer referenced.
            _fragments.Clear();
            _pendingFragmentsClear = false;
        }
        _dropSize = 0;
        if (_bufLen < LogFormat.HeaderSize) return NeedMore;
        ReadOnlySpan<byte> header = _backingStore.AsSpan(_bufStart, _bufLen);
        int length = header[4] | (header[5] << 8);
        byte type = header[6];
        int headerSize = LogFormat.HeaderSize;
        if (LogFormat.IsRecyclableType(type))
        {
            if (_firstRecordRead && !_recycled)
            {
                // A recycled log should have started with a recycled record.
                SetError(BadRecord);
                return FragmentReady;
            }
            _recycled = true;
            headerSize = LogFormat.RecyclableHeaderSize;
            if (_bufLen < LogFormat.RecyclableHeaderSize) return NeedMore;
            uint logNum = Coding.DecodeFixed32(header[7..]);
            if (logNum != unchecked((uint)_logNumber))
            {
                SetError(OldRecord);
                return FragmentReady;
            }
        }
        if (headerSize + length > _bufLen) return NeedMore;

        if (type == (byte)LogRecordType.Zero && length == 0)
        {
            _bufStart = _bufLen = 0;
            SetError(BadRecord);
            return FragmentReady;
        }

        if (_checksum)
        {
            uint expectedCrc = Crc32C.Unmask(Coding.DecodeFixed32(header));
            uint actualCrc = Crc32C.Value(header.Slice(6, length + headerSize - 6));
            if (actualCrc != expectedCrc)
            {
                _dropSize = _bufLen;
                _bufStart = _bufLen = 0;
                SetError(BadRecordChecksum);
                return FragmentReady;
            }
        }

        _lastFragmentOffset = _endOfBufferOffset - _bufLen;
        int payloadOffset = _bufStart + headerSize;
        _bufStart += headerSize + length;
        _bufLen -= headerSize + length;

        if (_uncompress is null || LogFormat.IsMetadataType(type))
        {
            _fragArray = _backingStore;
            _fragOffset = payloadOffset;
            _fragLength = length;
            _fragType = type;
            return FragmentReady;
        }
        PooledBufferWriter output = _uncompressedRecord!;
        output.Clear();
        if (!_uncompress.Uncompress(_backingStore.AsSpan(payloadOffset, length), output))
        {
            SetError(BadRecord);
            return FragmentReady;
        }
        System.Runtime.InteropServices.MemoryMarshal.TryGetArray(output.WrittenMemory, out ArraySegment<byte> seg);
        _fragArray = seg.Array ?? [];
        _fragOffset = seg.Offset;
        _fragLength = seg.Count;
        _fragType = type;
        return FragmentReady;
    }

    private void SetError(int code)
    {
        _fragType = code;
        _fragArray = [];
        _fragOffset = _fragLength = 0;
    }

    // ---- TryReadMore -------------------------------------------------------------------------------------------

    /// <summary>
    /// Port of FragmentBufferedReader::TryReadMore: reads the next block when the last read was a full block, else
    /// re-polls the partially read block (UnmarkEOF). Returns false on a read error.
    /// </summary>
    private bool TryReadMoreSync()
    {
        // EOF exactly at a block boundary (e.g. an empty or block-aligned WAL being tailed): nothing is buffered, so
        // UnmarkEOF has nothing to re-read and would only clear the flag, deferring the read of newly appended data to
        // the next call (a tailing reader would then lag one poll behind). Read the next block right away instead.
        if (_eof && _eofOffset == 0 && !_readError) _eof = false;
        if (!_eof && !_readError)
        {
            // Last read was a full read, so this is a trailer to skip.
            _bufStart = _bufLen = 0;
            int n;
            try
            {
                n = Math.Max(0, _file.Read(_backingStore.AsSpan(0, LogFormat.BlockSize)));
            }
            catch (Exception ex) when (IsIOError(ex))
            {
                return OnReadError(ex);
            }
            OnBlockRead(n);
            return true;
        }
        if (!_readError) UnmarkEofSync();
        return !_readError || OnReadErrorTail();
    }

    private async ValueTask<bool> TryReadMoreAsync(CancellationToken cancellationToken)
    {
        if (_eof && _eofOffset == 0 && !_readError) _eof = false; // see TryReadMoreSync
        if (!_eof && !_readError)
        {
            _bufStart = _bufLen = 0;
            int n;
            try
            {
                n = Math.Max(0, await _file.ReadAsync(_backingStore.AsMemory(0, LogFormat.BlockSize), cancellationToken).ConfigureAwait(false));
            }
            catch (Exception ex) when (IsIOError(ex))
            {
                return OnReadError(ex);
            }
            OnBlockRead(n);
            return true;
        }
        if (!_readError)
        {
            _eof = false;
            if (_eofOffset != 0)
            {
                int remaining = PrepareUnmarkEof();
                int added;
                try
                {
                    added = Math.Max(0, await _file.ReadAsync(_backingStore.AsMemory(_eofOffset, remaining), cancellationToken).ConfigureAwait(false));
                }
                catch (Exception ex) when (IsIOError(ex))
                {
                    _readError = true;
                    return OnReadErrorTail();
                }
                FinishUnmarkEof(added, remaining);
            }
        }
        return !_readError || OnReadErrorTail();
    }

    private void OnBlockRead(int n)
    {
        _endOfBufferOffset += n;
        _bufStart = 0;
        _bufLen = n;
        if (_pendingDiscard > 0)
        {
            // Reposition: the block starts before the resume offset.
            int d = Math.Min(_pendingDiscard, n);
            _bufStart = d;
            _bufLen = n - d;
            _pendingDiscard = 0;
        }
        if (n < LogFormat.BlockSize)
        {
            _eof = true;
            _eofOffset = n;
        }
    }

    private bool OnReadError(Exception ex)
    {
        _bufStart = _bufLen = 0;
        _reporter?.Corruption(LogFormat.BlockSize, StatusCode.IOError, ex.Message, LogReporter.UnspecifiedLogNumber);
        _readError = true;
        return false;
    }

    private bool OnReadErrorTail()
    {
        _bufStart = _bufLen = 0;
        return false;
    }

    /// <summary>Reader::UnmarkEOF: re-reads the rest of the partially read block.</summary>
    private void UnmarkEofSync()
    {
        _eof = false;
        if (_eofOffset == 0) return;
        int remaining = PrepareUnmarkEof();
        int added;
        try
        {
            added = Math.Max(0, _file.Read(_backingStore.AsSpan(_eofOffset, remaining)));
        }
        catch (Exception ex) when (IsIOError(ex))
        {
            _readError = true;
            return;
        }
        FinishUnmarkEof(added, remaining);
    }

    // consumed_bytes + buffer_size() + remaining == kBlockSize
    private int PrepareUnmarkEof()
    {
        int consumed = _eofOffset - _bufLen;
        if (_bufStart != consumed)
        {
            _backingStore.AsSpan(_bufStart, _bufLen).CopyTo(_backingStore.AsSpan(consumed));
            _bufStart = consumed;
        }
        return LogFormat.BlockSize - _eofOffset;
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

    private static bool IsIOError(Exception ex) =>
        ex is IOException || (ex is RocksDbException rex && rex.Code == StatusCode.IOError);

    private void InitCompression(CompressionType type)
    {
        _compressionType = type;
        _compressionTypeRecordRead = true;
        if (type == CompressionType.None) return;
        _uncompress ??= new WalZstdDecompressor();
        _uncompressedRecord ??= new PooledBufferWriter(LogFormat.BlockSize);
    }

    private void ReportCorruption(long bytes, string reason) =>
        _reporter?.Corruption(bytes, StatusCode.Corruption, reason, LogReporter.UnspecifiedLogNumber);

    public void Dispose()
    {
        byte[] b = _backingStore;
        _backingStore = [];
        if (b.Length != 0) ArrayPool<byte>.Shared.Return(b);
        _fragments.Dispose();
        _uncompressedRecord?.Dispose();
        _uncompress?.Dispose();
        _uncompress = null;
        _file.Dispose();
    }
}
