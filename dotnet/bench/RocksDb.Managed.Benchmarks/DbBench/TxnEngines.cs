using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>Concurrency control of the transaction benchmarks.</summary>
public enum TxnKind
{
    /// <summary>OptimisticTransactionDB (validate at commit; conflicts are retried).</summary>
    Optimistic,
    /// <summary>TransactionDB, WriteCommitted (per-key locks taken by each put).</summary>
    Pessimistic,
    /// <summary>TransactionDB, WritePrepared.</summary>
    WritePrepared,
    /// <summary>TransactionDB, WriteUnprepared (named transactions flushed to the DB at half of their puts).</summary>
    WriteUnprepared,
}

/// <summary>A transactional DB opened for the txnoptimistic / txnpessimistic benchmarks.</summary>
public interface ITxnEngine : IDisposable
{
    /// <summary>A per-thread session that reuses its transaction object.</summary>
    ITxnSession CreateSession();
}

/// <summary>Runs transactions of <c>count</c> puts (keys packed back to back in <c>keys</c>). Owned by one thread.</summary>
public interface ITxnSession : IDisposable
{
    /// <summary>Begins a transaction, puts every key, commits; returns false on a conflict (the caller retries).</summary>
    bool Run(ReadOnlySpan<byte> keys, int count, int keySize, ReadOnlySpan<byte> value);

    /// <summary>Async variant (managed only): lock waits and the commit don't block a thread.</summary>
    ValueTask<bool> RunAsync(ReadOnlyMemory<byte> keys, int count, int keySize, ReadOnlyMemory<byte> value);
}

public static class TxnEngineFactory
{
    /// <summary>The transaction kind of a transaction benchmark, or null.</summary>
    public static TxnKind? KindOf(DbBenchmark bench) => bench switch
    {
        DbBenchmark.TxnOptimistic => TxnKind.Optimistic,
        DbBenchmark.TxnPessimistic => TxnKind.Pessimistic,
        DbBenchmark.TxnWritePrepared => TxnKind.WritePrepared,
        DbBenchmark.TxnWriteUnprepared => TxnKind.WriteUnprepared,
        _ => null,
    };

    /// <param name="txnBytes">Approximate size of one transaction's batch (WriteUnprepared flushes at half of it).</param>
    public static ITxnEngine Open(EngineKind engine, TxnKind kind, string path, EngineOptions options, long txnBytes)
    {
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return engine switch
        {
            EngineKind.Managed => new ManagedTxnEngine(kind, path, options, txnBytes / 2),
            EngineKind.RocksDbSharp => new NativeTxnEngine(kind, path, options, txnBytes / 2),
            _ => throw new ArgumentException("engine " + BenchFlags.Name(engine) + " is not an in-process engine"),
        };
    }
}

/// <summary>Managed <see cref="OptimisticTransactionDatabase"/> (kValidateParallel, the default) or <see cref="TransactionDatabase"/>.</summary>
internal sealed class ManagedTxnEngine : ITxnEngine
{
    private readonly OptimisticTransactionDatabase? _occ;
    private readonly TransactionDatabase? _pessimistic;
    private readonly WriteOptions _wo;
    private readonly TransactionOptions _txnOptions;
    private readonly bool _named;
    private int _sessions;

    public ManagedTxnEngine(TxnKind kind, string path, EngineOptions options, long flushThreshold)
    {
        (DbOptions dbo, ColumnFamilyOptions cfo) = ManagedEngine.BuildOptions(options);
        ColumnFamilyDescriptor[] cfs = [new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)];
        _txnOptions = new TransactionOptions();
        if (kind == TxnKind.Optimistic)
        {
            _occ = OptimisticTransactionDatabase.Open(path, dbo, new OptimisticTransactionDbOptions(), cfs);
        }
        else
        {
            var tdo = new TransactionDbOptions
            {
                WritePolicy = kind switch
                {
                    TxnKind.WritePrepared => TransactionWritePolicy.WritePrepared,
                    TxnKind.WriteUnprepared => TransactionWritePolicy.WriteUnprepared,
                    _ => TransactionWritePolicy.WriteCommitted,
                },
            };
            if (kind == TxnKind.WriteUnprepared)
            {
                _txnOptions.WriteBatchFlushThreshold = Math.Max(flushThreshold, 1);
                _named = true; // unprepared batches can only be written by named transactions
            }
            _pessimistic = TransactionDatabase.Open(path, dbo, tdo, cfs);
        }
        _wo = new WriteOptions { Sync = options.Sync, DisableWal = options.DisableWal };
    }

    public ITxnSession CreateSession() => new Session(this);

    public void Dispose()
    {
        _occ?.Dispose();
        _pessimistic?.Dispose();
    }

    private sealed class Session(ManagedTxnEngine e) : ITxnSession
    {
        private static readonly OptimisticTransactionOptions s_occOptions = new();
        private Transaction? _txn;
        private readonly string _name = "s" + Interlocked.Increment(ref e._sessions).ToString(System.Globalization.CultureInfo.InvariantCulture);

        private Transaction Begin()
        {
            if (e._occ is { } occ) return _txn = _txn is null ? occ.BeginTransaction(e._wo, s_occOptions) : occ.BeginTransaction(e._wo, s_occOptions, _txn);
            TransactionDatabase db = e._pessimistic!;
            _txn = _txn is null ? db.BeginTransaction(e._wo, e._txnOptions) : db.BeginTransaction(e._wo, e._txnOptions, _txn);
            if (e._named) _txn.SetName(_name);
            return _txn;
        }

        public bool Run(ReadOnlySpan<byte> keys, int count, int keySize, ReadOnlySpan<byte> value)
        {
            Transaction txn = Begin();
            try
            {
                for (int i = 0; i < count; i++) txn.Put(keys.Slice(i * keySize, keySize), value);
                txn.Commit();
                return true;
            }
            catch (RocksDbException ex) when (ex.Code is StatusCode.Busy or StatusCode.TryAgain or StatusCode.TimedOut)
            {
                txn.Rollback();
                return false;
            }
        }

        public async ValueTask<bool> RunAsync(ReadOnlyMemory<byte> keys, int count, int keySize, ReadOnlyMemory<byte> value)
        {
            Transaction txn = Begin();
            try
            {
                for (int i = 0; i < count; i++) await txn.PutAsync(keys.Slice(i * keySize, keySize), value).ConfigureAwait(false);
                await txn.CommitAsync().ConfigureAwait(false);
                return true;
            }
            catch (RocksDbException ex) when (ex.Code is StatusCode.Busy or StatusCode.TryAgain or StatusCode.TimedOut)
            {
                txn.Rollback();
                return false;
            }
        }

        public void Dispose() => _txn?.Dispose();
    }
}

/// <summary>Native OptimisticTransactionDB / TransactionDB through the C API exposed by rocksdb-sharp.</summary>
internal sealed unsafe class NativeTxnEngine : ITxnEngine
{
    private static readonly RocksDbSharp.Native N = RocksDbSharp.Native.Instance;
    private readonly TxnKind _kind;
    private readonly RocksDbSharp.DbOptions _options;
    private readonly RocksDbSharp.Cache? _cache;
    private readonly IntPtr _hyperClock;
    private readonly RocksDbSharp.BloomFilterPolicy? _bloom;
    private readonly RocksDbSharp.BlockBasedTableOptions _table;
    private readonly RocksDbSharp.WriteOptions _wo;
    private readonly IntPtr _txnDbOptions, _otxnOptions, _txnOptions;
    private readonly IntPtr _db;
    private readonly bool _named;
    private int _sessions;

    public NativeTxnEngine(TxnKind kind, string path, EngineOptions options, long flushThreshold)
    {
        _kind = kind;
        _options = Native.NativeDbFactory.CreateOptions(options, out _cache, out _hyperClock, out _bloom, out _table);
        N.rocksdb_options_set_max_background_jobs(_options.Handle, options.MaxBackgroundJobs);
        _wo = new RocksDbSharp.WriteOptions().SetSync(options.Sync).DisableWal(options.DisableWal ? 1 : 0);
        if (kind == TxnKind.Optimistic)
        {
            _otxnOptions = N.rocksdb_optimistictransaction_options_create();
            _db = N.rocksdb_optimistictransactiondb_open(_options.Handle, path);
        }
        else
        {
            _txnDbOptions = N.rocksdb_transactiondb_options_create();
            _txnOptions = N.rocksdb_transaction_options_create();
            // TxnDBWritePolicy: WRITE_COMMITTED = 0, WRITE_PREPARED = 1, WRITE_UNPREPARED = 2.
            N.rocksdb_transactiondb_options_set_write_policy(_txnDbOptions, kind switch
            {
                TxnKind.WritePrepared => 1,
                TxnKind.WriteUnprepared => 2,
                _ => 0,
            });
            if (kind == TxnKind.WriteUnprepared)
            {
                N.rocksdb_transaction_options_set_write_batch_flush_threshold(_txnOptions, Math.Max(flushThreshold, 1));
                _named = true;
            }
            _db = N.rocksdb_transactiondb_open(_options.Handle, _txnDbOptions, path);
        }
    }

    public ITxnSession CreateSession() => new Session(this);

    public void Dispose()
    {
        if (_kind == TxnKind.Optimistic)
        {
            N.rocksdb_optimistictransactiondb_close(_db);
            N.rocksdb_optimistictransaction_options_destroy(_otxnOptions);
        }
        else
        {
            N.rocksdb_transactiondb_close(_db);
            N.rocksdb_transaction_options_destroy(_txnOptions);
            N.rocksdb_transactiondb_options_destroy(_txnDbOptions);
        }
        if (_hyperClock != IntPtr.Zero) N.rocksdb_cache_destroy(_hyperClock);
        GC.KeepAlive(_cache);
        GC.KeepAlive(_bloom);
        GC.KeepAlive(_table);
    }

    private sealed class Session(NativeTxnEngine e) : ITxnSession
    {
        private IntPtr _txn;
        private readonly byte[] _name = System.Text.Encoding.ASCII.GetBytes("s" + Interlocked.Increment(ref e._sessions).ToString(System.Globalization.CultureInfo.InvariantCulture));

        public bool Run(ReadOnlySpan<byte> keys, int count, int keySize, ReadOnlySpan<byte> value)
        {
            _txn = e._kind == TxnKind.Optimistic
                ? N.rocksdb_optimistictransaction_begin(e._db, e._wo.Handle, e._otxnOptions, _txn)
                : N.rocksdb_transaction_begin(e._db, e._wo.Handle, e._txnOptions, _txn);
            IntPtr err = IntPtr.Zero;
            if (e._named)
            {
                fixed (byte* nm = _name) N.rocksdb_transaction_set_name(_txn, (IntPtr)nm, (UIntPtr)_name.Length, out err);
            }
            fixed (byte* k = keys)
            fixed (byte* v = value)
            {
                for (int i = 0; i < count && err == IntPtr.Zero; i++)
                {
                    N.rocksdb_transaction_put(_txn, k + i * keySize, (UIntPtr)keySize, v, (UIntPtr)value.Length, out err);
                }
            }
            if (err == IntPtr.Zero) N.rocksdb_transaction_commit(_txn, out err);
            if (err == IntPtr.Zero) return true;
            // Busy / TryAgain / TimedOut: roll back and let the caller retry.
            N.rocksdb_free(err);
            N.rocksdb_transaction_rollback(_txn, out IntPtr err2);
            if (err2 != IntPtr.Zero) N.rocksdb_free(err2);
            return false;
        }

        public ValueTask<bool> RunAsync(ReadOnlyMemory<byte> keys, int count, int keySize, ReadOnlyMemory<byte> value) =>
            throw new NotSupportedException("--async is only supported by --engine managed");

        public void Dispose()
        {
            if (_txn != IntPtr.Zero) N.rocksdb_transaction_destroy(_txn);
        }
    }
}
