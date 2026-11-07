using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// File layer micro-benchmarks: buffered appends (WAL-like 100-byte records), append+fdatasync, and 4 KiB random
/// reads (page-cache resident) through the sync and async paths. Steady-state paths should not allocate.
/// </summary>
[MemoryDiagnoser]
public class EnvBenchmarks
{
    private const int RecordSize = 100;
    private const int FileSize = 64 << 20;

    private string _dir = null!;
    private IWritableFile _writer = null!;
    private IRandomAccessFile _reader = null!;
    private readonly byte[] _record = new byte[RecordSize];
    private readonly byte[] _readBuffer = new byte[4096];
    private Random _rnd = null!;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-managed-envbench-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        new Random(1).NextBytes(_record);
        string data = Path.Combine(_dir, "data");
        using (IWritableFile w = LocalFileSystem.Instance.CreateWritable(data, true, WritableFileOptions.Default))
        {
            byte[] chunk = new byte[1 << 20];
            new Random(2).NextBytes(chunk);
            for (int i = 0; i < FileSize / chunk.Length; i++) w.Append(chunk);
        }
        _reader = LocalFileSystem.Instance.OpenRandomAccess(data);
        _writer = LocalFileSystem.Instance.CreateWritable(Path.Combine(_dir, "wal"), true, WritableFileOptions.Default);
        _rnd = new Random(3);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _writer.Dispose();
        _reader.Dispose();
        Directory.Delete(_dir, recursive: true);
    }

    [Benchmark]
    public void Append100() => _writer.Append(_record);

    [Benchmark]
    public ValueTask AppendAsync100() => _writer.AppendAsync(_record);

    [Benchmark]
    public void Append100AndSync()
    {
        _writer.Append(_record);
        _writer.Sync();
    }

    [Benchmark]
    public int RandomRead4K() => _reader.Read(NextOffset(), _readBuffer);

    [Benchmark]
    public ValueTask<int> RandomRead4KAsync() => _reader.ReadAsync(NextOffset(), _readBuffer);

    private long NextOffset() => (long)_rnd.Next(0, FileSize / 4096 - 1) * 4096;
}
