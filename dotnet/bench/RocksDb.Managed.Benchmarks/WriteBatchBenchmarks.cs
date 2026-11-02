using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Benchmarks;

/// <summary>WriteBatch building and iteration: managed port vs native (rocksdb-sharp) WriteBatch building.</summary>
[MemoryDiagnoser]
public class WriteBatchBenchmarks
{
    private const int Ops = 100;
    private readonly byte[][] _keys = new byte[Ops][];
    private readonly byte[] _value = new byte[100];
    private WriteBatch _managed = null!;
    private RocksDbSharp.WriteBatch _native = null!;
    private byte[] _rep = [];

    [GlobalSetup]
    public void Setup()
    {
        var rnd = new Random(42);
        for (int i = 0; i < Ops; i++)
        {
            _keys[i] = new byte[16];
            rnd.NextBytes(_keys[i]);
        }
        rnd.NextBytes(_value);
        _managed = new WriteBatch();
        _native = new RocksDbSharp.WriteBatch();
        BuildManaged();
        _rep = _managed.Data.ToArray();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _managed.Dispose();
        _native.Dispose();
    }

    [Benchmark(Baseline = true, Description = "Managed: Clear + 100 Put (16B key, 100B value)")]
    public int BuildManaged()
    {
        WriteBatch b = _managed;
        b.Clear();
        for (int i = 0; i < Ops; i++) b.Put(_keys[i], _value);
        return b.DataSize;
    }

    [Benchmark(Description = "Managed: 100 Put to CF 7")]
    public int BuildManagedCf()
    {
        WriteBatch b = _managed;
        b.Clear();
        for (int i = 0; i < Ops; i++) b.Put(7, _keys[i], _value);
        return b.DataSize;
    }

    [Benchmark(Description = "Native: Clear + 100 Put (16B key, 100B value)")]
    public int BuildNative()
    {
        RocksDbSharp.WriteBatch b = _native;
        b.Clear();
        for (int i = 0; i < Ops; i++) b.Put(_keys[i], _value);
        return b.Count();
    }

    [Benchmark(Description = "Managed: iterate 100 records (ref struct reader)")]
    public int IterateReader()
    {
        int sum = 0;
        foreach (WriteBatchEntry e in new WriteBatchReader(_rep)) sum += e.Key.Length + e.Value.Length;
        return sum;
    }

    [Benchmark(Description = "Managed: iterate 100 records (struct handler)")]
    public int IterateHandler()
    {
        var h = new SumHandler();
        WriteBatch.Iterate(_rep, ref h);
        return h.Sum;
    }

    private struct SumHandler : IWriteBatchHandler
    {
        public int Sum;
        public bool Continue() => true;
        public void Put(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Sum += k.Length + v.Length;
        public void TimedPut(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v, ulong t) => Sum += k.Length + v.Length;
        public void PutEntity(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> e) => Sum += k.Length + e.Length;
        public void Delete(uint cf, ReadOnlySpan<byte> k) => Sum += k.Length;
        public void SingleDelete(uint cf, ReadOnlySpan<byte> k) => Sum += k.Length;
        public void DeleteRange(uint cf, ReadOnlySpan<byte> b, ReadOnlySpan<byte> e) => Sum += b.Length + e.Length;
        public void Merge(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Sum += k.Length + v.Length;
        public void PutBlobIndex(uint cf, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => Sum += k.Length + v.Length;
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> ts) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }
}
