using RocksDb.Managed.Utilities.Transactions;

namespace RocksDb.Managed.Benchmarks.DbBench;

/// <summary>Concurrency control of the transaction benchmarks.</summary>
public enum TxnKind
{
    /// <summary>OptimisticTransactionDB (validate at commit; conflicts are retried).</summary>
    Optimistic,
    /// <summary>TransactionDB, WriteCommitted (per-key locks taken by each put).</summary>
    Pessimistic,
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
    public static ITxnEngine Open(EngineKind engine, TxnKind kind, string path, EngineOptions options)
    {
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        Directory.CreateDirectory(path);
        return engine switch
        {
            EngineKind.Managed => new ManagedTxnEngine(kind, path, options),
            EngineKind.RocksDbSharp => new NativeTxnEngine(kind, path, options),
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

    public ManagedTxnEngine(TxnKind kind, string path, EngineOptions options)
    {
        (DbOptions dbo, ColumnFamilyOptions cfo) = ManagedEngine.BuildOptions(options);
        ColumnFamilyDescriptor[] cfs = [new(ColumnFamilyDescriptor.DefaultColumnFamilyName, cfo)];
        if (kind == TxnKind.Optimistic) _occ = OptimisticTransactionDatabase.Open(path, dbo, new OptimisticTransactionDbOptions(), cfs);
        else _pessimistic = TransactionDatabase.Open(path, dbo, new TransactionDbOptions(), cfs);
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
        private static readonly TransactionOptions s_txnOptions = new();
        private Transaction? _txn;

        private Transaction Begin()
        {
            if (e._occ is { } occ) return _txn = _txn is null ? occ.BeginTransaction(e._wo, s_occOptions) : occ.BeginTransaction(e._wo, s_occOptions, _txn);
            TransactionDatabase db = e._pessimistic!;
            return _txn = _txn is null ? db.BeginTransaction(e._wo, s_txnOptions) : db.BeginTransaction(e._wo, s_txnOptions, _txn);
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

    public NativeTxnEngine(TxnKind kind, string path, EngineOptions options)
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

        public bool Run(ReadOnlySpan<byte> keys, int count, int keySize, ReadOnlySpan<byte> value)
        {
            _txn = e._kind == TxnKind.Optimistic
                ? N.rocksdb_optimistictransaction_begin(e._db, e._wo.Handle, e._otxnOptions, _txn)
                : N.rocksdb_transaction_begin(e._db, e._wo.Handle, e._txnOptions, _txn);
            IntPtr err = IntPtr.Zero;
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
