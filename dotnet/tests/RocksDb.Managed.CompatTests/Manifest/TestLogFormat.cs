using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.CompatTests.Manifest;

/// <summary>
/// TEST-ONLY minimal reader for the log format (db/log_format.h): 32KiB blocks, 7-byte header
/// crc32c-masked(4) + length(2) + type(1); types Full=1/First=2/Middle=3/Last=4; zero trailer padding.
/// TODO: replace with stream A's Wal.LogReader adapter once it lands.
/// </summary>
internal sealed class TestLogReader : IManifestRecordReader
{
    internal const int BlockSize = 32768;
    internal const int HeaderSize = 7;
    private readonly byte[] _data;
    private int _pos;
    private byte[] _scratch = new byte[256];

    public TestLogReader(byte[] data) => _data = data;

    public bool TryReadRecord(out ReadOnlySpan<byte> record)
    {
        int len = 0;
        bool inFragmented = false;
        while (true)
        {
            int blockLeft = BlockSize - _pos % BlockSize;
            if (blockLeft < HeaderSize)
            {
                _pos += blockLeft; // zero trailer
                continue;
            }
            if (_pos + HeaderSize > _data.Length)
            {
                record = default;
                return false; // EOF (a truncated trailing header is treated as EOF)
            }
            ReadOnlySpan<byte> h = _data.AsSpan(_pos, HeaderSize);
            uint crc = Coding.DecodeFixed32(h);
            int length = h[4] | (h[5] << 8);
            byte type = h[6];
            if (type == 0 && length == 0)
            {
                // Zero padding (preallocated space) -> skip rest of block.
                _pos += blockLeft;
                if (_pos >= _data.Length) { record = default; return false; }
                continue;
            }
            if (_pos + HeaderSize + length > _data.Length) throw new InvalidDataException("truncated record");
            ReadOnlySpan<byte> payload = _data.AsSpan(_pos + HeaderSize, length);
            uint actual = Crc32C.Extend(Crc32C.Value([type]), payload);
            if (Crc32C.Unmask(crc) != actual) throw new InvalidDataException("checksum mismatch at " + _pos);
            _pos += HeaderSize + length;
            switch (type)
            {
                case 1:
                    record = payload;
                    return true;
                case 2:
                    len = 0;
                    Append(ref len, payload);
                    inFragmented = true;
                    break;
                case 3:
                    if (!inFragmented) throw new InvalidDataException("middle without first");
                    Append(ref len, payload);
                    break;
                case 4:
                    if (!inFragmented) throw new InvalidDataException("last without first");
                    Append(ref len, payload);
                    record = _scratch.AsSpan(0, len);
                    return true;
                default:
                    throw new InvalidDataException("unexpected record type " + type);
            }
        }
    }

    private void Append(ref int len, ReadOnlySpan<byte> payload)
    {
        if (_scratch.Length < len + payload.Length) Array.Resize(ref _scratch, Math.Max(_scratch.Length * 2, len + payload.Length));
        payload.CopyTo(_scratch.AsSpan(len));
        len += payload.Length;
    }
}

/// <summary>
/// TEST-ONLY minimal log-format writer (db/log_writer.cc, non-recyclable). TODO: replace with stream A's Wal.LogWriter.
/// </summary>
internal sealed class TestLogWriter : IManifestRecordWriter, IDisposable
{
    private readonly FileStream _file;
    private int _blockOffset;

    public TestLogWriter(string path) => _file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);

    public ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken)
    {
        ReadOnlySpan<byte> left = record.Span;
        bool begin = true;
        Span<byte> header = stackalloc byte[TestLogReader.HeaderSize];
        do
        {
            int leftover = TestLogReader.BlockSize - _blockOffset;
            if (leftover < TestLogReader.HeaderSize)
            {
                if (leftover > 0) _file.Write(new byte[leftover]);
                _blockOffset = 0;
            }
            int avail = TestLogReader.BlockSize - _blockOffset - TestLogReader.HeaderSize;
            int fragment = Math.Min(left.Length, avail);
            bool end = fragment == left.Length;
            byte type = begin && end ? (byte)1 : begin ? (byte)2 : end ? (byte)4 : (byte)3;
            uint crc = Crc32C.Mask(Crc32C.Extend(Crc32C.Value([type]), left[..fragment]));
            Coding.EncodeFixed32(header, crc);
            header[4] = (byte)fragment;
            header[5] = (byte)(fragment >> 8);
            header[6] = type;
            _file.Write(header);
            _file.Write(left[..fragment]);
            _blockOffset += TestLogReader.HeaderSize + fragment;
            left = left[fragment..];
            begin = false;
        } while (left.Length > 0);
        return ValueTask.CompletedTask;
    }

    public ValueTask SyncAsync(CancellationToken cancellationToken)
    {
        _file.Flush(flushToDisk: true);
        return ValueTask.CompletedTask;
    }

    public void Dispose() => _file.Dispose();
}
