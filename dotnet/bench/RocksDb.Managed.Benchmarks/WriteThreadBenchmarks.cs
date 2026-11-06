using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Benchmarks;

/// <summary>
/// Write group formation (WriteThread::JoinBatchGroup + EnterAsBatchGroupLeader + ExitAsBatchGroupLeader, the part
/// of a group commit that runs under the write thread's lock), single-threaded so the cost is measured without
/// scheduling noise: a leader and <see cref="Followers"/> queued compatible writers form one group.
/// </summary>
[MemoryDiagnoser]
public class WriteThreadBenchmarks
{
    private WriteThread _thread = null!;
    private WriteThread.Writer[] _writers = [];
    private readonly List<WriteThread.Writer> _group = new();
    private WriteBatch _batch = null!;

    [Params(1, 7, 31)]
    public int Followers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _thread = new WriteThread(1 << 20, 0);
        _batch = new WriteBatch();
        _batch.Put("0123456789abcdef"u8, new byte[100]);
        _writers = new WriteThread.Writer[Followers + 1];
        for (int i = 0; i < _writers.Length; i++) _writers[i] = new WriteThread.Writer();
    }

    [GlobalCleanup]
    public void Cleanup() => _batch.Dispose();

    [Benchmark(Description = "Join + EnterAsBatchGroupLeader + ExitAsBatchGroupLeader")]
    public int FormGroup()
    {
        WriteThread.Writer[] ws = _writers;
        for (int i = 0; i < ws.Length; i++) ws[i].Prepare(_batch, null, syncWaiter: true);
        _thread.JoinBatchGroup(ws[0]);
        for (int i = 1; i < ws.Length; i++) _thread.JoinBatchGroup(ws[i]);
        _thread.EnterAsBatchGroupLeader(ws[0], _group);
        int n = _group.Count;
        _thread.ExitAsBatchGroupLeader(_group);
        return n;
    }
}
