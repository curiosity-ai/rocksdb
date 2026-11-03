using System.Buffers;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tracing;

/// <summary>
/// Sink of encoded trace records (include/rocksdb/trace_reader_writer.h TraceWriter). Calls are serialized by the
/// tracer that owns it. <see cref="RocksDatabase.EndTrace"/> (and the other End*Trace calls) dispose it.
/// </summary>
public abstract class TraceWriter : IDisposable
{
    /// <summary>Appends one encoded record.</summary>
    public abstract void Write(ReadOnlySpan<byte> data);

    /// <summary>Bytes written so far (compared against the max trace file size).</summary>
    public abstract ulong FileSize { get; }

    /// <summary>Flushes and closes the sink.</summary>
    public virtual void Close()
    {
    }

    public void Dispose()
    {
        Close();
        GC.SuppressFinalize(this);
    }

    /// <summary>NewFileTraceWriter: a writer creating (truncating) <paramref name="path"/> on <paramref name="fileSystem"/>.</summary>
    public static TraceWriter CreateFile(IFileSystem fileSystem, string path)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        return new FileTraceWriter(fileSystem.CreateWritable(path));
    }

    /// <summary>A writer on the local file system.</summary>
    public static TraceWriter CreateFile(string path) => CreateFile(LocalFileSystem.Instance, path);
}

/// <summary>Port of FileTraceWriter (utilities/trace/file_trace_reader_writer.cc).</summary>
internal sealed class FileTraceWriter(IWritableFile file) : TraceWriter
{
    private IWritableFile? _file = file;

    public override void Write(ReadOnlySpan<byte> data) => (_file ?? throw RocksDbException.IOError("TraceWriter is closed.")).Append(data);

    public override ulong FileSize => (ulong)(_file?.Size ?? 0);

    public override void Close()
    {
        IWritableFile? f = Interlocked.Exchange(ref _file, null);
        if (f is null) return;
        f.Flush();
        f.Dispose();
    }
}

/// <summary>
/// Source of encoded trace records (include/rocksdb/trace_reader_writer.h TraceReader). Not thread-safe; the
/// <see cref="Replayer"/> serializes its reads.
/// </summary>
public abstract class TraceReader : IDisposable
{
    /// <summary>
    /// Appends the next encoded record (metadata + payload) to <paramref name="destination"/>. Returns false at the end
    /// of the data (upstream: Status::Incomplete); throws Corruption for a truncated record.
    /// </summary>
    public abstract bool TryRead(IBufferWriter<byte> destination);

    /// <summary>Rewinds to the first record.</summary>
    public abstract void Reset();

    public virtual void Close()
    {
    }

    public void Dispose()
    {
        Close();
        GC.SuppressFinalize(this);
    }

    /// <summary>NewFileTraceReader: a reader of <paramref name="path"/> on <paramref name="fileSystem"/>.</summary>
    public static TraceReader OpenFile(IFileSystem fileSystem, string path)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        return new FileTraceReader(fileSystem.OpenRandomAccess(path));
    }

    /// <summary>A reader on the local file system.</summary>
    public static TraceReader OpenFile(string path) => OpenFile(LocalFileSystem.Instance, path);

    /// <summary>Reads the next record into a new array (null at the end of the data).</summary>
    internal byte[]? ReadRecord()
    {
        var w = new ArrayBufferWriter<byte>(256);
        return TryRead(w) ? w.WrittenSpan.ToArray() : null;
    }
}

/// <summary>Port of FileTraceReader (utilities/trace/file_trace_reader_writer.cc).</summary>
internal sealed class FileTraceReader(IRandomAccessFile file) : TraceReader
{
    private IRandomAccessFile? _file = file;
    private long _offset;

    public override bool TryRead(IBufferWriter<byte> destination)
    {
        IRandomAccessFile f = _file ?? throw RocksDbException.IOError("TraceReader is closed.");
        Span<byte> meta = stackalloc byte[TraceFormat.MetadataSize];
        int n = ReadFully(f, _offset, meta);
        if (n == 0) return false; // no more data
        if (n < TraceFormat.MetadataSize) throw RocksDbException.Corruption("Corrupted trace file.");
        uint payloadLength = Coding.DecodeFixed32(meta[(TraceFormat.TimestampSize + TraceFormat.TypeSize)..]);
        if (_offset + TraceFormat.MetadataSize + payloadLength > f.Length) throw RocksDbException.Corruption("Corrupted trace file.");
        destination.Write(meta);
        Span<byte> payload = destination.GetSpan((int)payloadLength)[..(int)payloadLength];
        if (ReadFully(f, _offset + TraceFormat.MetadataSize, payload) < payloadLength) throw RocksDbException.Corruption("Corrupted trace file.");
        destination.Advance((int)payloadLength);
        _offset += TraceFormat.MetadataSize + payloadLength;
        return true;
    }

    private static int ReadFully(IRandomAccessFile f, long offset, Span<byte> dst)
    {
        int total = 0;
        while (total < dst.Length)
        {
            int r = f.Read(offset + total, dst[total..]);
            if (r <= 0) break;
            total += r;
        }
        return total;
    }

    public override void Reset()
    {
        if (_file is null) throw RocksDbException.IOError("TraceReader is closed.");
        _offset = 0;
    }

    public override void Close() => Interlocked.Exchange(ref _file, null)?.Dispose();
}
