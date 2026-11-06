using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Benchmarks;

// Merge hot paths: one full merge through MergeHelper (the FullMergeV3 dispatch reads, flush and compaction use) and a
// merge-heavy compaction. "Allocated" must be "-" for FullMerge.
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories Merge

/// <summary>MergeHelper.TimedFullMerge with no / a plain base value over 4 operands (UInt64Add and StringAppend).</summary>
[MemoryDiagnoser]
[BenchmarkCategory("Merge")]
public class FullMergeBenchmarks
{
    private readonly MergeOperandBuffer _u64 = new();
    private readonly MergeOperandBuffer _str = new();
    private readonly PooledBufferWriter _out = new(256);
    private readonly byte[] _base = new byte[8];
    private readonly byte[] _key = "key000001"u8.ToArray();

    [GlobalSetup]
    public void Setup()
    {
        _u64Op = MergeOperator.UInt64Add;
        _appendOp = MergeOperator.StringAppend(",");
        for (int i = 0; i < 4; i++)
        {
            byte[] v = new byte[8];
            Coding.EncodeFixed64(v, (ulong)i);
            _u64.PushNewer(v);
            _str.PushNewer("operand-value-24-bytes.."u8);
        }
    }

    /// <summary>Baseline: the pre-FullMergeV3 MergeHelper.TimedFullMerge body (a direct FullMerge call).</summary>
    [Benchmark(Baseline = true)]
    public int UInt64AddDirectFullMerge()
    {
        _out.Clear();
        if (!_u64Op.FullMerge(_key, true, _base, _u64.AsOperands(), _out)) throw new InvalidOperationException();
        return _out.WrittenCount;
    }

    [Benchmark]
    public int StringAppendDirectFullMerge()
    {
        _out.Clear();
        if (!_appendOp.FullMerge(_key, false, default, _str.AsOperands(), _out)) throw new InvalidOperationException();
        return _out.WrittenCount;
    }

    [Benchmark]
    public int UInt64AddWithBase()
    {
        _out.Clear();
        MergeHelper.TimedFullMerge(_u64Op, _key, true, _base, _u64.AsOperands(), _out);
        return _out.WrittenCount;
    }

    // Instance fields (not static readonly): like the column family operator in reads and compaction, the call is not devirtualized.
    private MergeOperator _u64Op = null!;
    private MergeOperator _appendOp = null!;

    [Benchmark]
    public int StringAppendNoBase()
    {
        _out.Clear();
        MergeHelper.TimedFullMerge(_appendOp, _key, false, default, _str.AsOperands(), _out);
        return _out.WrittenCount;
    }
}

/// <summary>
/// A bottommost CompactRange over 4 L0 files where every key has 4 merge operands (UInt64Add) on a base value: one
/// MergeUntil + full merge per key. One operation = one compaction of <see cref="Keys"/> keys.
/// </summary>
[MemoryDiagnoser]
[BenchmarkCategory("Merge")]
[SimpleJob(RunStrategy.Monitoring, launchCount: 1, warmupCount: 3, iterationCount: 15)]
public class MergeCompactionBenchmarks
{
    private const int Keys = 50_000;
    private RocksDatabase _db = null!;

    [IterationSetup]
    public void Setup()
    {
        var fs = new MemoryFileSystem();
        var cf = new ColumnFamilyOptions { MergeOperator = MergeOperator.UInt64Add, DisableAutoCompactions = true, WriteBufferSize = 256UL << 20 };
        _db = RocksDatabase.Open("/bench", new DbOptions { CreateIfMissing = true, FileSystem = fs }, cf);
        byte[] v = new byte[8];
        byte[] key = new byte[10];
        for (int round = 0; round < 5; round++)
        {
            for (int i = 0; i < Keys; i++)
            {
                "k"u8.CopyTo(key);
                System.Text.Encoding.ASCII.GetBytes(i.ToString("D9", System.Globalization.CultureInfo.InvariantCulture), key.AsSpan(1));
                Coding.EncodeFixed64(v, (ulong)(i + round));
                if (round == 0) _db.Put(key, v);
                else _db.Merge(key, v);
            }
            _db.Flush();
        }
    }

    [Benchmark]
    public void CompactMergeChains() => _db.CompactRange();

    [IterationCleanup]
    public void Cleanup() => _db.Dispose();
}
