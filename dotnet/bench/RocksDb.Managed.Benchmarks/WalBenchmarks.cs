using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Benchmarks;

/// <summary>Log writer/reader throughput on in-memory files (isolates the format code from disk I/O).</summary>
[MemoryDiagnoser]
public class WalBenchmarks
{
    /// <summary>Discards appended data (keeps only the size), like a fast WritableFileWriter.</summary>
    private sealed class NullWritableFile : IWritableFile
    {
        public long Size { get; private set; }
        public void Append(ReadOnlySpan<byte> data) => Size += data.Length;
        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Size += data.Length;
            return ValueTask.CompletedTask;
        }
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public void Flush() { }
        public void Sync() { }
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Re-readable in-memory file.</summary>
    private sealed class MemorySequentialFile(byte[] data) : ISequentialFile
    {
        private int _pos;
        public void Reset() => _pos = 0;
        public int Read(Span<byte> destination)
        {
            int n = Math.Min(destination.Length, data.Length - _pos);
            data.AsSpan(_pos, n).CopyTo(destination);
            _pos += n;
            return n;
        }
        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(Read(destination.Span));
        public void Skip(long bytes) => _pos += (int)bytes;
        public void Dispose() { }
    }

    private const int RecordsPerLog = 1000;
    private readonly byte[] _record = new byte[1024];
    private LogWriter _writer = null!;
    private LogWriter _recyclableWriter = null!;
    private byte[] _log = [];

    [GlobalSetup]
    public void Setup()
    {
        new Random(1).NextBytes(_record);
        _writer = new LogWriter(new NullWritableFile(), 1, recycleLogFiles: false);
        _recyclableWriter = new LogWriter(new NullWritableFile(), 1, recycleLogFiles: true);
        var file = new MemoryFile();
        using (var w = new LogWriter(file, 1, recycleLogFiles: false, manualFlush: true, CompressionType.None, 0))
        {
            for (int i = 0; i < RecordsPerLog; i++) w.AddRecord(_record);
        }
        _log = file.ToArray();
    }

    private sealed class MemoryFile : IWritableFile
    {
        private readonly MemoryStream _ms = new();
        public long Size => _ms.Length;
        public void Append(ReadOnlySpan<byte> data) => _ms.Write(data);
        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) { _ms.Write(data.Span); return ValueTask.CompletedTask; }
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask SyncAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public void Flush() { }
        public void Sync() { }
        public byte[] ToArray() => _ms.ToArray();
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _writer.Dispose();
        _recyclableWriter.Dispose();
    }

    [Benchmark(Description = "LogWriter.AddRecord 1KiB")]
    public void AddRecord() => _writer.AddRecord(_record);

    [Benchmark(Description = "LogWriter.AddRecord 1KiB (recyclable)")]
    public void AddRecordRecyclable() => _recyclableWriter.AddRecord(_record);

    [Benchmark(Description = "LogWriter.AddRecordAsync 1KiB")]
    public ValueTask AddRecordAsync() => _writer.AddRecordAsync(_record);

    [Benchmark(OperationsPerInvoke = RecordsPerLog, Description = "LogReader.TryReadRecord 1KiB (per record, incl. reader setup)")]
    public int ReadAll()
    {
        var file = new MemorySequentialFile(_log);
        using var reader = new LogReader(file, null, checksum: true, 1);
        int n = 0;
        while (reader.TryReadRecord(out ReadOnlySpan<byte> r)) n += r.Length;
        return n;
    }
}
