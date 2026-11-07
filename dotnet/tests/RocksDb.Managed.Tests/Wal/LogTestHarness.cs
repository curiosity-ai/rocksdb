using System.Text;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Tests.Wal;

/// <summary>In-memory append-only file (test::StringSink). Optionally overwrites existing content (OverwritingStringSink).</summary>
internal sealed class MemoryWritableFile : IWritableFile
{
    private readonly List<byte> _contents;
    private int _overwritePos;
    private readonly bool _overwrite;

    public MemoryWritableFile(List<byte> contents, bool overwrite)
    {
        _contents = contents;
        _overwrite = overwrite;
    }

    public long Size => _overwrite ? _overwritePos : _contents.Count;
    public int FlushCount { get; private set; }

    public void Append(ReadOnlySpan<byte> data)
    {
        foreach (byte b in data)
        {
            if (_overwrite && _overwritePos < _contents.Count) _contents[_overwritePos] = b;
            else _contents.Add(b);
            _overwritePos++;
        }
    }

    public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        Append(data.Span);
        return ValueTask.CompletedTask;
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default) { FlushCount++; return ValueTask.CompletedTask; }
    public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
    public void Flush() => FlushCount++;
    public void Sync() { }
    public void Dispose() { }
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// In-memory sequential file over a shared byte list (log_test.cc StringSource): supports forced errors and forced
/// EOF at a position, and observes data appended after creation (for UnmarkEof tests).
/// </summary>
internal sealed class MemorySequentialFile : ISequentialFile
{
    private readonly List<byte> _contents;
    private int _pos;
    public bool ForceError;
    public int ForceErrorPosition;
    public bool ForceEof;
    public int ForceEofPosition;
    public bool YieldAsync;

    public MemorySequentialFile(List<byte> contents) => _contents = contents;

    public int Read(Span<byte> destination)
    {
        int n = destination.Length;
        if (ForceError)
        {
            if (ForceErrorPosition >= n)
            {
                ForceErrorPosition -= n;
            }
            else
            {
                // The C++ source returns the partial data together with the error; an exception cannot carry data,
                // so the bytes before the error position are skipped (consumed) instead.
                _pos += ForceErrorPosition;
                ForceError = false;
                throw new IOException("read error");
            }
        }
        int available = _contents.Count - _pos;
        if (available < n) n = available;
        if (ForceEof)
        {
            if (ForceEofPosition >= n)
            {
                ForceEofPosition -= n;
            }
            else
            {
                ForceEof = false;
                n = ForceEofPosition;
            }
        }
        for (int i = 0; i < n; i++) destination[i] = _contents[_pos + i];
        _pos += n;
        return n;
    }

    public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (YieldAsync) await Task.Yield();
        return Read(destination.Span);
    }

    public void Skip(long bytes) => _pos = (int)Math.Min(_contents.Count, _pos + bytes);
    public void Dispose() { }
}

internal sealed class ReportCollector : LogReporter
{
    public long DroppedBytes;
    public readonly StringBuilder Message = new();
    public int OldRecords;

    public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber)
    {
        DroppedBytes += bytes;
        Message.Append(code == StatusCode.Corruption ? "Corruption: " : "IO error: ").Append(reason);
    }

    public override void OldLogRecord(long bytes) => OldRecords++;
}

/// <summary>Port of the LogTest fixture (db/log_test.cc).</summary>
internal sealed class LogHarness : IDisposable
{
    public const ulong LogNumber = 123;
    public readonly List<byte> Contents = new();
    public readonly MemorySequentialFile Source;
    public readonly ReportCollector Report = new();
    public readonly LogWriter Writer;
    public readonly LogReader Reader;
    public readonly bool Recyclable;
    public readonly bool Async;
    public int HeaderSize => Recyclable ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;

    public LogHarness(bool recyclable, bool async = false, CompressionType compression = CompressionType.None)
    {
        Recyclable = recyclable;
        Async = async;
        Writer = new LogWriter(new MemoryWritableFile(Contents, overwrite: false), LogNumber, recyclable, manualFlush: false,
            compression, initialBlockOffset: 0);
        Writer.AddCompressionTypeRecord();
        Source = new MemorySequentialFile(Contents) { YieldAsync = async };
        Reader = new LogReader(Source, Report, checksum: true, LogNumber);
    }

    public void Write(string msg) => Write(Encoding.ASCII.GetBytes(msg));

    public void Write(byte[] msg)
    {
        if (Async) Writer.AddRecordAsync(msg).AsTask().GetAwaiter().GetResult();
        else Writer.AddRecord(msg);
    }

    public string Read(WalRecoveryMode mode = WalRecoveryMode.TolerateCorruptedTailRecords)
    {
        Reader.RecoveryMode = mode;
        if (Async)
        {
            bool ok = Reader.ReadRecordAsync().AsTask().GetAwaiter().GetResult();
            return ok ? Encoding.ASCII.GetString(Reader.Record.Span) : "EOF";
        }
        return Reader.TryReadRecord(out ReadOnlySpan<byte> record) ? Encoding.ASCII.GetString(record) : "EOF";
    }

    public int WrittenBytes => Contents.Count;
    public void IncrementByte(int offset, int delta) => Contents[offset] = unchecked((byte)(Contents[offset] + delta));
    public void SetByte(int offset, byte b) => Contents[offset] = b;
    public void ShrinkSize(int bytes) => Contents.RemoveRange(Contents.Count - bytes, bytes);

    public void FixChecksum(int headerOffset, int len, bool recyclable)
    {
        // Compute crc of type/len/data
        int headerSize = recyclable ? LogFormat.RecyclableHeaderSize : LogFormat.HeaderSize;
        byte[] data = Contents.GetRange(headerOffset + 6, headerSize - 6 + len).ToArray();
        uint crc = Managed.Util.Crc32C.Mask(Managed.Util.Crc32C.Value(data));
        Span<byte> c = stackalloc byte[4];
        Managed.Util.Coding.EncodeFixed32(c, crc);
        for (int i = 0; i < 4; i++) Contents[headerOffset + i] = c[i];
    }

    public void ForceError(int position = 0)
    {
        Source.ForceError = true;
        Source.ForceErrorPosition = position;
    }

    public void ForceEof(int position = 0)
    {
        Source.ForceEof = true;
        Source.ForceEofPosition = position;
    }

    public void UnmarkEof()
    {
        if (Async) Reader.UnmarkEofAsync().AsTask().GetAwaiter().GetResult();
        else Reader.UnmarkEof();
    }

    public long DroppedBytes => Report.DroppedBytes;
    public string ReportMessage => Report.Message.ToString();
    public bool MatchError(string msg) => ReportMessage.Contains(msg, StringComparison.Ordinal);

    public void Dispose()
    {
        Writer.Dispose();
        Reader.Dispose();
    }

    // ---- string helpers from log_test.cc ----

    /// <summary>Construct a string of the specified length made out of the supplied partial string.</summary>
    public static string BigString(string partial, int n)
    {
        var sb = new StringBuilder(n);
        while (sb.Length < n) sb.Append(partial);
        sb.Length = n;
        return sb.ToString();
    }

    /// <summary>Construct a string from a number.</summary>
    public static string NumberString(int n) => n.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".";
}
