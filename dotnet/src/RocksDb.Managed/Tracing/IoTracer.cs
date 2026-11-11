using System.Buffers;
using System.Diagnostics;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Tracing;

/// <summary>Optional fields of an IO trace record, by bit of <see cref="IoTraceRecord.IoOpData"/> (io_tracer.h IOTraceOp).</summary>
[Flags]
public enum IoTraceOp : ulong
{
    None = 0,
    FileSize = 1 << 0,
    Length = 1 << 1,
    Offset = 1 << 2,
}

/// <summary>One file system operation (trace_replay/io_tracer.h IOTraceRecord). Timestamps and latency are nanoseconds.</summary>
public sealed class IoTraceRecord
{
    public ulong AccessTimestamp { get; set; }
    public TraceType TraceType { get; set; } = TraceType.IoTracer;
    /// <summary>Which of <see cref="FileSize"/>, <see cref="Length"/>, <see cref="Offset"/> are recorded.</summary>
    public IoTraceOp IoOpData { get; set; }
    /// <summary>The FileSystem / file method name, as upstream's __func__ (e.g. "Read", "Append", "NewRandomAccessFile").</summary>
    public string FileOperation { get; set; } = "";
    public ulong Latency { get; set; }
    /// <summary>The status string ("OK" or the error).</summary>
    public string IoStatus { get; set; } = "OK";
    /// <summary>The file name without its directory.</summary>
    public string FileName { get; set; } = "";
    public ulong Length { get; set; }
    public ulong Offset { get; set; }
    public ulong FileSize { get; set; }
    /// <summary>IODebugContext trace data bits (request id). Always 0 for managed traces.</summary>
    public ulong TraceData { get; set; }
    public string RequestId { get; set; } = "";
}

/// <summary>
/// Port of IOTracer + IOTraceWriter (trace_replay/io_tracer.cc). One per DB; the DB's file system wrapper
/// (<see cref="IoTracingFileSystem"/>) checks <see cref="IsEnabled"/> (one volatile read) per file operation.
/// </summary>
internal sealed class IoTracer
{
    private readonly Lock _lock = new();
    private volatile TraceWriter? _writer;
    private ulong _maxFileSize;
    private readonly PooledBufferWriter _payload = new(256);
    private readonly PooledBufferWriter _record = new(256);

    public bool IsEnabled => _writer is not null;

    /// <summary>IOTracer::StartIOTrace: Busy if a trace is already running; writes the header.</summary>
    public void StartTrace(TraceOptions options, TraceWriter writer)
    {
        lock (_lock)
        {
            if (_writer is not null) throw new RocksDbException(StatusCode.Busy, "Busy: ");
            _maxFileSize = options.MaxTraceFileSize;
            _payload.Clear();
            Coding.PutLengthPrefixedSlice(_payload, TraceFormat.MagicBytes);
            Coding.PutFixed32(_payload, (uint)OptionsFile.CurrentRocksDbVersion.Major);
            Coding.PutFixed32(_payload, (uint)OptionsFile.CurrentRocksDbVersion.Minor);
            _record.Clear();
            TraceFormat.EncodeTrace(_record, TraceFormat.NowMicros(), TraceType.Begin, _payload.WrittenSpan);
            writer.Write(_record.WrittenSpan);
            _writer = writer;
        }
    }

    /// <summary>IOTracer::EndIOTrace.</summary>
    public void EndTrace()
    {
        lock (_lock)
        {
            TraceWriter? w = _writer;
            if (w is null) return;
            _writer = null;
            w.Close();
        }
    }

    /// <summary>IOTraceWriter::WriteIOOp (status ignored, as upstream).</summary>
    public void WriteIoOp(string operation, long startTimestamp, string? error, string path, IoTraceOp ops, ulong length, ulong offset, ulong fileSize)
    {
        ulong latency = (ulong)((UInt128)(ulong)(Stopwatch.GetTimestamp() - startTimestamp) * 1_000_000_000UL / (ulong)Stopwatch.Frequency);
        int slash = path.AsSpan().LastIndexOfAny('/', '\\');
        ReadOnlySpan<char> name = path.AsSpan(slash + 1);
        lock (_lock)
        {
            TraceWriter? w = _writer;
            if (w is null || w.FileSize > _maxFileSize) return;
            _payload.Clear();
            Coding.PutFixed64(_payload, (ulong)ops);
            TraceFormat.PutLengthPrefixedUtf8(_payload, operation);
            Coding.PutFixed64(_payload, latency);
            TraceFormat.PutLengthPrefixedUtf8(_payload, error ?? "OK");
            TraceFormat.PutLengthPrefixedUtf8(_payload, name);
            // Optional fields in bit order (kIOFileSize, kIOLen, kIOOffset).
            if ((ops & IoTraceOp.FileSize) != 0) Coding.PutFixed64(_payload, fileSize);
            if ((ops & IoTraceOp.Length) != 0) Coding.PutFixed64(_payload, length);
            if ((ops & IoTraceOp.Offset) != 0) Coding.PutFixed64(_payload, offset);
            Coding.PutFixed64(_payload, 0); // trace_data: no IODebugContext request ids
            _record.Clear();
            TraceFormat.EncodeTrace(_record, TraceFormat.NowNanos(), TraceType.IoTracer, _payload.WrittenSpan);
            try
            {
                w.Write(_record.WrittenSpan);
            }
            catch (Exception)
            {
                // IOTracer::WriteIOOp ignores the writer's status.
            }
        }
    }

    internal static string StatusOf(Exception e) => e is RocksDbException r ? r.Message : "IO error: " + e.Message;
}

/// <summary>Port of IOTraceReader (trace_replay/io_tracer.cc): reads an IO trace file.</summary>
public sealed class IoTraceReader(TraceReader reader) : IDisposable
{
    private readonly ArrayBufferWriter<byte> _buffer = new(256);

    /// <summary>IOTraceReader::ReadHeader.</summary>
    public TraceFileHeader ReadHeader() => BlockCacheTraceReader.ReadTraceFileHeader(reader, _buffer);

    /// <summary>IOTraceReader::ReadIOOp: false at the end of the trace; throws Incomplete for a truncated record.</summary>
    public bool TryReadIoOp(out IoTraceRecord? record)
    {
        record = null;
        _buffer.Clear();
        if (!reader.TryRead(_buffer)) return false;
        if (!TraceFormat.TryDecodeTrace(_buffer.WrittenMemory, out TraceEntry trace)) throw new RocksDbException(StatusCode.Incomplete, "Incomplete: Decode trace string failed");
        ReadOnlySpan<byte> p = trace.Payload.Span;
        var r = new IoTraceRecord { AccessTimestamp = trace.Timestamp, TraceType = trace.Type };
        if (!Coding.GetFixed64(ref p, out ulong ops)) throw Incomplete("io op data");
        r.IoOpData = (IoTraceOp)ops;
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> op)) throw Incomplete("file operation");
        r.FileOperation = System.Text.Encoding.UTF8.GetString(op);
        if (!Coding.GetFixed64(ref p, out ulong latency)) throw Incomplete("latency");
        r.Latency = latency;
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> status)) throw Incomplete("IO status");
        r.IoStatus = System.Text.Encoding.UTF8.GetString(status);
        if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> name)) throw Incomplete("file name");
        r.FileName = System.Text.Encoding.UTF8.GetString(name);
        ulong bits = ops;
        while (bits != 0)
        {
            int pos = System.Numerics.BitOperations.TrailingZeroCount(bits);
            if (!Coding.GetFixed64(ref p, out ulong v)) throw Incomplete("io op field");
            switch (pos)
            {
                case 0: r.FileSize = v; break;
                case 1: r.Length = v; break;
                case 2: r.Offset = v; break;
                default: throw RocksDbException.Corruption("unknown IO trace op " + pos);
            }
            bits &= bits - 1;
        }
        if (!Coding.GetFixed64(ref p, out ulong traceData)) throw Incomplete("trace op");
        r.TraceData = traceData;
        if ((traceData & 1) != 0)
        {
            if (!Coding.GetLengthPrefixedSlice(ref p, out ReadOnlySpan<byte> rid)) throw Incomplete("request id");
            r.RequestId = System.Text.Encoding.UTF8.GetString(rid);
        }
        record = r;
        return true;
    }

    private static RocksDbException Incomplete(string what) => new(StatusCode.Incomplete, "Incomplete: Incomplete access record: Failed to read " + what + ".");

    public void Dispose() => reader.Dispose();
}

/// <summary>
/// Port of FileSystemTracingWrapper / FSRandomAccessFileTracingWrapper / FSWritableFileTracingWrapper /
/// FSSequentialFileTracingWrapper (env/file_system_tracer.cc): the DB's file system, recording every operation while
/// IO tracing runs. Each operation costs one extra virtual call and one <see cref="IoTracer.IsEnabled"/> check when
/// tracing is off. Operation names are upstream's (__func__) so that the native trace analyzer groups them identically.
/// </summary>
internal sealed class IoTracingFileSystem(IFileSystem inner, IoTracer tracer) : IFileSystem
{
    public IFileSystem Inner => inner;

    private void Trace(string op, long start, Exception? e, string path, IoTraceOp ops = IoTraceOp.None, ulong len = 0, ulong offset = 0, ulong size = 0) =>
        tracer.WriteIoOp(op, start, e is null ? null : IoTracer.StatusOf(e), path, ops, len, offset, size);

    public IWritableFile CreateWritable(string path, bool truncate = true) =>
        new TracedWritable(Traced("NewWritableFile", path, () => inner.CreateWritable(path, truncate)), tracer, path);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) =>
        new TracedWritable(Traced("NewWritableFile", path, () => inner.CreateWritable(path, truncate, options)), tracer, path);

    public IWritableFile ReopenWritable(string path, WritableFileOptions options) =>
        new TracedWritable(Traced("ReopenWritableFile", path, () => inner.ReopenWritable(path, options)), tracer, path);

    public IRandomAccessFile OpenRandomAccess(string path) =>
        new TracedRandomAccess(Traced("NewRandomAccessFile", path, () => inner.OpenRandomAccess(path)), tracer, path);

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) =>
        new TracedRandomAccess(Traced("NewRandomAccessFile", path, () => inner.OpenRandomAccess(path, options)), tracer, path);

    public ISequentialFile OpenSequential(string path) =>
        new TracedSequential(Traced("NewSequentialFile", path, () => inner.OpenSequential(path)), tracer, path);

    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) =>
        new TracedSequential(Traced("NewSequentialFile", path, () => inner.OpenSequential(path, options)), tracer, path);

    private T Traced<T>(string op, string path, Func<T> f)
    {
        if (!tracer.IsEnabled) return f();
        long start = Stopwatch.GetTimestamp();
        try
        {
            T r = f();
            Trace(op, start, null, path);
            return r;
        }
        catch (Exception e)
        {
            Trace(op, start, e, path);
            throw;
        }
    }

    private void Traced(string op, string path, Action f)
    {
        if (!tracer.IsEnabled)
        {
            f();
            return;
        }
        long start = Stopwatch.GetTimestamp();
        try
        {
            f();
            Trace(op, start, null, path);
        }
        catch (Exception e)
        {
            Trace(op, start, e, path);
            throw;
        }
    }

    public bool FileExists(string path) => Traced("FileExists", path, () => inner.FileExists(path));

    public long GetFileSize(string path)
    {
        if (!tracer.IsEnabled) return inner.GetFileSize(path);
        long start = Stopwatch.GetTimestamp();
        try
        {
            long size = inner.GetFileSize(path);
            Trace("GetFileSize", start, null, path, IoTraceOp.FileSize, size: (ulong)size);
            return size;
        }
        catch (Exception e)
        {
            Trace("GetFileSize", start, e, path);
            throw;
        }
    }

    public void DeleteFile(string path) => Traced("DeleteFile", path, () => inner.DeleteFile(path));
    public void RenameFile(string source, string target) => Traced("RenameFile", source, () => inner.RenameFile(source, target));
    public void CreateDirectoryIfMissing(string path) => Traced("CreateDirIfMissing", path, () => inner.CreateDirectoryIfMissing(path));
    public IEnumerable<string> GetChildren(string directory) => Traced("GetChildren", directory, () => inner.GetChildren(directory));
    public IDisposable LockFile(string path) => inner.LockFile(path);
    public void SyncDirectory(string path) => Traced("Fsync", path, () => inner.SyncDirectory(path));
    public bool DirectoryExists(string path) => inner.DirectoryExists(path);
    public void DeleteDirectory(string path) => Traced("DeleteDir", path, () => inner.DeleteDirectory(path));
    public void LinkFile(string source, string target) => Traced("LinkFile", source, () => inner.LinkFile(source, target));
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
    public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
    public long GetFreeSpace(string path) => inner.GetFreeSpace(path);

    private sealed class TracedRandomAccess(IRandomAccessFile inner, IoTracer tracer, string path) : IRandomAccessFile
    {
        public long Length => inner.Length;

        public int Read(long offset, Span<byte> destination)
        {
            if (!tracer.IsEnabled) return inner.Read(offset, destination);
            long start = Stopwatch.GetTimestamp();
            try
            {
                int n = inner.Read(offset, destination);
                tracer.WriteIoOp("Read", start, null, path, IoTraceOp.Length | IoTraceOp.Offset, (ulong)n, (ulong)offset, 0);
                return n;
            }
            catch (Exception e)
            {
                tracer.WriteIoOp("Read", start, IoTracer.StatusOf(e), path, IoTraceOp.Length | IoTraceOp.Offset, (ulong)destination.Length, (ulong)offset, 0);
                throw;
            }
        }

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            tracer.IsEnabled ? ReadAsyncTraced(offset, destination, cancellationToken) : inner.ReadAsync(offset, destination, cancellationToken);

        [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<int> ReadAsyncTraced(long offset, Memory<byte> destination, CancellationToken ct)
        {
            long start = Stopwatch.GetTimestamp();
            try
            {
                int n = await inner.ReadAsync(offset, destination, ct).ConfigureAwait(false);
                tracer.WriteIoOp("ReadAsync", start, null, path, IoTraceOp.Length | IoTraceOp.Offset, (ulong)n, (ulong)offset, 0);
                return n;
            }
            catch (Exception e)
            {
                tracer.WriteIoOp("ReadAsync", start, IoTracer.StatusOf(e), path, IoTraceOp.Length | IoTraceOp.Offset, (ulong)destination.Length, (ulong)offset, 0);
                throw;
            }
        }

        public bool Prefetch(long offset, long length)
        {
            if (!tracer.IsEnabled) return inner.Prefetch(offset, length);
            long start = Stopwatch.GetTimestamp();
            bool r = inner.Prefetch(offset, length);
            tracer.WriteIoOp("Prefetch", start, r ? null : "Not implemented: Prefetch", path, IoTraceOp.Length | IoTraceOp.Offset, (ulong)length, (ulong)offset, 0);
            return r;
        }

        public void Dispose() => inner.Dispose();
    }

    private sealed class TracedSequential(ISequentialFile inner, IoTracer tracer, string path) : ISequentialFile
    {
        public int Read(Span<byte> destination)
        {
            if (!tracer.IsEnabled) return inner.Read(destination);
            long start = Stopwatch.GetTimestamp();
            int n = inner.Read(destination);
            tracer.WriteIoOp("Read", start, null, path, IoTraceOp.Length, (ulong)n, 0, 0);
            return n;
        }

        public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            if (!tracer.IsEnabled) return await inner.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
            long start = Stopwatch.GetTimestamp();
            int n = await inner.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
            tracer.WriteIoOp("Read", start, null, path, IoTraceOp.Length, (ulong)n, 0, 0);
            return n;
        }

        public void Skip(long bytes)
        {
            if (!tracer.IsEnabled)
            {
                inner.Skip(bytes);
                return;
            }
            long start = Stopwatch.GetTimestamp();
            inner.Skip(bytes);
            tracer.WriteIoOp("Skip", start, null, path, IoTraceOp.Length, (ulong)bytes, 0, 0);
        }

        public void Dispose() => inner.Dispose();
    }

    private sealed class TracedWritable(IWritableFile inner, IoTracer tracer, string path) : IWritableFile, ITruncatableWritableFile
    {
        public long Size => inner.Size;

        /// <summary>Forwarded when the wrapped file supports it (not traced); a no-op otherwise.</summary>
        public void Truncate(long size)
        {
            if (inner is ITruncatableWritableFile t) t.Truncate(size);
        }

        public void Append(ReadOnlySpan<byte> data)
        {
            if (!tracer.IsEnabled)
            {
                inner.Append(data);
                return;
            }
            long start = Stopwatch.GetTimestamp();
            inner.Append(data);
            tracer.WriteIoOp("Append", start, null, path, IoTraceOp.Length, (ulong)data.Length, 0, 0);
        }

        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            tracer.IsEnabled ? TracedAsync("Append", inner.AppendAsync(data, cancellationToken), IoTraceOp.Length, (ulong)data.Length) : inner.AppendAsync(data, cancellationToken);

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
            tracer.IsEnabled ? TracedAsync("Flush", inner.FlushAsync(cancellationToken), IoTraceOp.None, 0) : inner.FlushAsync(cancellationToken);

        public ValueTask SyncAsync(CancellationToken cancellationToken = default) =>
            tracer.IsEnabled ? TracedAsync("Sync", inner.SyncAsync(cancellationToken), IoTraceOp.None, 0) : inner.SyncAsync(cancellationToken);

        private async ValueTask TracedAsync(string op, ValueTask t, IoTraceOp ops, ulong len)
        {
            long start = Stopwatch.GetTimestamp();
            try
            {
                await t.ConfigureAwait(false);
                tracer.WriteIoOp(op, start, null, path, ops, len, 0, 0);
            }
            catch (Exception e)
            {
                tracer.WriteIoOp(op, start, IoTracer.StatusOf(e), path, ops, len, 0, 0);
                throw;
            }
        }

        public void Flush() => TracedSync("Flush", static f => f.Flush());
        public void Sync() => TracedSync("Sync", static f => f.Sync());

        private void TracedSync(string op, Action<IWritableFile> a)
        {
            if (!tracer.IsEnabled)
            {
                a(inner);
                return;
            }
            long start = Stopwatch.GetTimestamp();
            try
            {
                a(inner);
                tracer.WriteIoOp(op, start, null, path, IoTraceOp.None, 0, 0, 0);
            }
            catch (Exception e)
            {
                tracer.WriteIoOp(op, start, IoTracer.StatusOf(e), path, IoTraceOp.None, 0, 0, 0);
                throw;
            }
        }

        public void Dispose()
        {
            if (!tracer.IsEnabled)
            {
                inner.Dispose();
                return;
            }
            long start = Stopwatch.GetTimestamp();
            inner.Dispose();
            tracer.WriteIoOp("Close", start, null, path, IoTraceOp.None, 0, 0, 0);
        }

        public ValueTask DisposeAsync() => tracer.IsEnabled ? TracedAsync("Close", inner.DisposeAsync(), IoTraceOp.None, 0) : inner.DisposeAsync();
    }
}
