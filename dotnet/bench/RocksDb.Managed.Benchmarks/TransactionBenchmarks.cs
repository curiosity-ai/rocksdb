using BenchmarkDotNet.Attributes;
using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.Benchmarks;

// Steady-state cost of a short transaction (Begin reusing the previous transaction object, 4 puts, Commit) per
// concurrency control / write policy, next to a plain 4-put WriteBatch written through the same DB (the floor: WAL +
// memtable). Allocated is per transaction; it should be (near) zero once lock entries, tracked keys and per-transaction
// objects are recycled.
//
//   dotnet run -c Release --project bench/RocksDb.Managed.Benchmarks -f net10.0 -- --anyCategories Txn

/// <summary>Which transactional DB a <see cref="TransactionBenchmarks"/> run uses.</summary>
public enum TxnBenchKind
{
    /// <summary>OptimisticTransactionDB (kValidateParallel).</summary>
    Optimistic,
    /// <summary>TransactionDB, WriteCommitted.</summary>
    Pessimistic,
    /// <summary>TransactionDB, WritePrepared (single write queue).</summary>
    WritePrepared,
    /// <summary>TransactionDB, WritePrepared with two_write_queues.</summary>
    WritePreparedTwoQueues,
}

/// <summary>Begin/Put/Commit cycles on each transaction kind (sync API, one thread).</summary>
[MemoryDiagnoser]
[BenchmarkCategory("Txn")]
public class TransactionBenchmarks
{
    private const int KeyCount = 4096;
    private const int PutsPerTxn = 4;
    private const int TxnsPerInvoke = 1000;

    [Params(TxnBenchKind.Optimistic, TxnBenchKind.Pessimistic, TxnBenchKind.WritePrepared, TxnBenchKind.WritePreparedTwoQueues)]
    public TxnBenchKind Kind { get; set; }

    private string _dir = "";
    private OptimisticTransactionDatabase? _occ;
    private TransactionDatabase? _pessimistic;
    private RocksDatabase _db = null!;
    private readonly WriteOptions _wo = new();
    private readonly OptimisticTransactionOptions _occOptions = new();
    private readonly TransactionOptions _txnOptions = new();
    private Transaction? _txn;
    private WriteBatch _batch = null!;
    private byte[][] _keys = null!;
    private readonly byte[] _value = new byte[100];
    private int _next;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-txnbench-" + Environment.ProcessId + "-" + Kind);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        var dbo = new DbOptions { CreateIfMissing = true, TwoWriteQueues = Kind == TxnBenchKind.WritePreparedTwoQueues };
        // A memtable large enough that no flush runs during the measurement (its allocations would be counted).
        var cfo = new ColumnFamilyOptions { WriteBufferSize = 1UL << 30 };
        ColumnFamilyDescriptor[] cfs = [new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)];
        if (Kind == TxnBenchKind.Optimistic)
        {
            _occ = OptimisticTransactionDatabase.Open(_dir, dbo, new OptimisticTransactionDbOptions(), cfs);
            _db = _occ.Database;
        }
        else
        {
            var tdo = new TransactionDbOptions
            {
                WritePolicy = Kind == TxnBenchKind.Pessimistic ? TransactionWritePolicy.WriteCommitted : TransactionWritePolicy.WritePrepared,
            };
            _pessimistic = TransactionDatabase.Open(_dir, dbo, tdo, cfs);
            _db = _pessimistic.Database;
        }
        _keys = new byte[KeyCount][];
        var rnd = new Random(42);
        for (int i = 0; i < KeyCount; i++)
        {
            _keys[i] = new byte[16];
            rnd.NextBytes(_keys[i]);
        }
        rnd.NextBytes(_value);
        _batch = new WriteBatch();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _txn?.Dispose();
        _batch.Dispose();
        _occ?.Dispose();
        _pessimistic?.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private byte[] NextKey()
    {
        byte[] k = _keys[_next];
        _next = (_next + 1) & (KeyCount - 1);
        return k;
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Description = "Begin + 4 Put + Commit")]
    public void BeginPutCommit()
    {
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            Transaction txn = _occ is { } occ
                ? _txn = _txn is null ? occ.BeginTransaction(_wo, _occOptions) : occ.BeginTransaction(_wo, _occOptions, _txn)
                : _txn = _txn is null ? _pessimistic!.BeginTransaction(_wo, _txnOptions) : _pessimistic!.BeginTransaction(_wo, _txnOptions, _txn);
            for (int i = 0; i < PutsPerTxn; i++) txn.Put(NextKey(), _value);
            txn.Commit();
        }
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Description = "2PC: Begin + 4 Put + Prepare + Commit")]
    public void BeginPutPrepareCommit()
    {
        if (_pessimistic is null) return; // optimistic transactions have no 2PC
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            Transaction txn = _txn = _txn is null ? _pessimistic.BeginTransaction(_wo, _txnOptions) : _pessimistic.BeginTransaction(_wo, _txnOptions, _txn);
            txn.SetName("xid");
            for (int i = 0; i < PutsPerTxn; i++) txn.Put(NextKey(), _value);
            txn.Prepare();
            txn.Commit();
        }
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Description = "Begin + 4 PutAsync + CommitAsync")]
    public async Task BeginPutCommitAsync()
    {
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            Transaction txn = _occ is { } occ
                ? _txn = _txn is null ? occ.BeginTransaction(_wo, _occOptions) : occ.BeginTransaction(_wo, _occOptions, _txn)
                : _txn = _txn is null ? _pessimistic!.BeginTransaction(_wo, _txnOptions) : _pessimistic!.BeginTransaction(_wo, _txnOptions, _txn);
            for (int i = 0; i < PutsPerTxn; i++) await txn.PutAsync(NextKey(), _value);
            await txn.CommitAsync();
        }
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Baseline = true, Description = "WriteBatch of 4 Put (no txn)")]
    public void PlainBatch()
    {
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            _batch.Clear();
            for (int i = 0; i < PutsPerTxn; i++) _batch.Put(NextKey(), _value);
            _db.Write(_wo, _batch);
        }
    }
}

/// <summary>Which lock-table mutexes a <see cref="PointLockBenchmarks"/> run uses.</summary>
public enum TxnMutexKind
{
    /// <summary>The built-in stripe locks (TransactionDbOptions.CustomMutexFactory = null).</summary>
    Builtin,
    /// <summary>TransactionDbMutexFactory.Default through the custom-mutex path.</summary>
    CustomDefault,
}

/// <summary>
/// Point-lock acquire/release cost of a pessimistic (WriteCommitted) transaction without writes: Begin, lock 4 keys
/// (GetForUpdate of missing keys / Put into the transaction's batch), Rollback releases them. Measures the lock manager
/// with the built-in stripe locks and with a custom mutex factory (TransactionDbOptions.CustomMutexFactory).
/// </summary>
[MemoryDiagnoser]
[BenchmarkCategory("Txn")]
public class PointLockBenchmarks
{
    private const int KeyCount = 4096;
    private const int LocksPerTxn = 4;
    private const int TxnsPerInvoke = 1000;

    [Params(TxnMutexKind.Builtin, TxnMutexKind.CustomDefault)]
    public TxnMutexKind Mutex { get; set; }

    private string _dir = "";
    private TransactionDatabase _db = null!;
    private readonly WriteOptions _wo = new();
    private readonly TransactionOptions _txnOptions = new();
    private readonly System.Buffers.ArrayBufferWriter<byte> _sink = new();
    private Transaction? _txn;
    private byte[][] _keys = null!;
    private readonly byte[] _value = new byte[100];
    private int _next;

    [GlobalSetup]
    public void Setup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "rocksdb-lockbench-" + Environment.ProcessId + "-" + Mutex);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, recursive: true);
        var tdo = new TransactionDbOptions { CustomMutexFactory = Mutex == TxnMutexKind.CustomDefault ? TransactionDbMutexFactory.Default : null };
        _db = TransactionDatabase.Open(_dir, new DbOptions { CreateIfMissing = true }, tdo,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);
        _keys = new byte[KeyCount][];
        var rnd = new Random(42);
        for (int i = 0; i < KeyCount; i++)
        {
            _keys[i] = new byte[16];
            rnd.NextBytes(_keys[i]);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _txn?.Dispose();
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch (IOException) { }
    }

    private byte[] NextKey()
    {
        byte[] k = _keys[_next];
        _next = (_next + 1) & (KeyCount - 1);
        return k;
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Description = "Begin + 4 GetForUpdate (missing) + Rollback")]
    public void GetForUpdateRollback()
    {
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            Transaction txn = _txn = _txn is null ? _db.BeginTransaction(_wo, _txnOptions) : _db.BeginTransaction(_wo, _txnOptions, _txn);
            for (int i = 0; i < LocksPerTxn; i++)
            {
                _sink.ResetWrittenCount();
                txn.TryGetForUpdate(null, NextKey(), _sink);
            }
            txn.Rollback();
        }
    }

    [Benchmark(OperationsPerInvoke = TxnsPerInvoke, Description = "Begin + 4 Put + Rollback")]
    public void PutRollback()
    {
        for (int t = 0; t < TxnsPerInvoke; t++)
        {
            Transaction txn = _txn = _txn is null ? _db.BeginTransaction(_wo, _txnOptions) : _db.BeginTransaction(_wo, _txnOptions, _txn);
            for (int i = 0; i < LocksPerTxn; i++) txn.Put(NextKey(), _value);
            txn.Rollback();
        }
    }
}
