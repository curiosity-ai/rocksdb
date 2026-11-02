using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A database with pessimistic concurrency control (TransactionDB, WriteCommitted policy). Port of
/// utilities/transactions/pessimistic_transaction_db.cc:
/// <list type="bullet">
/// <item>Transactions lock the keys they write or read for update (per-key locks in a striped lock table, with
/// timeouts, expiration and optional deadlock detection).</item>
/// <item>Writes made directly on this object (<see cref="Put(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>,
/// <see cref="Write(WriteOptions?, WriteBatch)"/>...) also take the locks of their keys, so they never interleave with
/// a transaction holding them. Reads and other operations go through <see cref="Database"/> (writes made there bypass
/// the locks).</item>
/// <item>Two-phase commit: a named transaction can be prepared (written to the WAL as a prepared section) and committed
/// or rolled back later, also after a crash: prepared transactions are recovered from the WAL on open
/// (<see cref="GetAllPreparedTransactions"/>). The WAL format matches native RocksDB, so either implementation can
/// recover the other's prepared transactions.</item>
/// </list>
/// Opening enables <c>allow_2pc</c> and memtable history (<c>max_write_buffer_size_to_maintain</c> = -1 when 0), like
/// TransactionDB::Open. Dispose closes the database; prepared transactions stay in the WAL.
/// </summary>
public sealed class TransactionDatabase : IDisposable, IAsyncDisposable
{
    private readonly RocksDatabase _db;
    private readonly TransactionDbOptions _options;
    private readonly Dictionary<string, PessimisticTransaction> _transactions = new(StringComparer.Ordinal);
    private readonly Lock _nameMapLock = new();
    private readonly Dictionary<ulong, PessimisticTransaction> _expirable = new();
    private readonly Lock _expirableLock = new();

    private TransactionDatabase(RocksDatabase db, TransactionDbOptions options)
    {
        _db = db;
        _options = options;
        LockManager = new PointLockManager(this, options);
    }

    /// <summary>The underlying database (reads, column families, flush...). Writes made there bypass the transaction locks.</summary>
    public RocksDatabase Database => _db;

    /// <summary>The options this database was opened with.</summary>
    public TransactionDbOptions Options => _options;

    internal PointLockManager LockManager { get; }

    // =====================================================================================================
    // Open
    // =====================================================================================================

    public static TransactionDatabase Open(string path, DbOptions options) =>
        Open(path, options, new TransactionDbOptions(), [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);

    public static TransactionDatabase Open(string path, DbOptions options, TransactionDbOptions transactionDbOptions) =>
        Open(path, options, transactionDbOptions, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())]);

    public static TransactionDatabase Open(string path, DbOptions options, TransactionDbOptions transactionDbOptions, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        DbOptions o = PrepareWrap(options, transactionDbOptions);
        RocksDatabase db = RocksDatabase.Open(path, o, OptimisticTransactionDatabase.PrepareColumnFamilies(columnFamilies));
        return Initialize(db, transactionDbOptions);
    }

    public static ValueTask<TransactionDatabase> OpenAsync(string path, DbOptions options, CancellationToken cancellationToken = default) =>
        OpenAsync(path, options, new TransactionDbOptions(),
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], cancellationToken);

    public static async ValueTask<TransactionDatabase> OpenAsync(string path, DbOptions options, TransactionDbOptions transactionDbOptions,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, CancellationToken cancellationToken = default)
    {
        DbOptions o = PrepareWrap(options, transactionDbOptions);
        RocksDatabase db = await RocksDatabase.OpenAsync(path, o, OptimisticTransactionDatabase.PrepareColumnFamilies(columnFamilies), cancellationToken)
            .ConfigureAwait(false);
        return Initialize(db, transactionDbOptions);
    }

    /// <summary>TransactionDB::PrepareWrap: enables allow_2pc (on a copy of the options).</summary>
    private static DbOptions PrepareWrap(DbOptions options, TransactionDbOptions transactionDbOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(transactionDbOptions);
        if (transactionDbOptions.WritePolicy != TransactionWritePolicy.WriteCommitted)
        {
            throw new RocksDbException(StatusCode.NotSupported, "Not implemented: only the WriteCommitted transaction write policy is supported");
        }
        DbOptions o = options.Clone();
        o.Allow2Pc = true;
        return o;
    }

    /// <summary>
    /// PessimisticTransactionDB::Initialize: creates 'real' transactions from the prepared transactions recovered from
    /// the WAL (in the prepared state, without locks: skip_concurrency_control).
    /// </summary>
    private static TransactionDatabase Initialize(RocksDatabase db, TransactionDbOptions options)
    {
        var txnDb = new TransactionDatabase(db, options);
        try
        {
            foreach (RecoveredTransaction r in db.Impl.TakeRecoveredTransactions())
            {
                try
                {
                    var txn = new PessimisticTransaction(txnDb, new WriteOptions { Sync = true }, new TransactionOptions { SkipConcurrencyControl = true });
                    txn.RebuildPrepared(r);
                }
                finally
                {
                    r.Batch.Dispose();
                }
            }
        }
        catch
        {
            db.Dispose();
            throw;
        }
        return txnDb;
    }

    // =====================================================================================================
    // Transactions
    // =====================================================================================================

    /// <summary>Starts a transaction (BeginTransaction).</summary>
    public Transaction BeginTransaction(WriteOptions writeOptions) => BeginTransaction(writeOptions, new TransactionOptions());

    public Transaction BeginTransaction(WriteOptions writeOptions, TransactionOptions transactionOptions)
    {
        ArgumentNullException.ThrowIfNull(transactionOptions);
        return new PessimisticTransaction(this, writeOptions, transactionOptions);
    }

    /// <summary>
    /// Reuses <paramref name="oldTransaction"/> (committed, rolled back or unused) for a new transaction instead of
    /// allocating one (BeginTransaction with old_txn).
    /// </summary>
    public Transaction BeginTransaction(WriteOptions writeOptions, TransactionOptions transactionOptions, Transaction oldTransaction)
    {
        ArgumentNullException.ThrowIfNull(transactionOptions);
        if (oldTransaction is not PessimisticTransaction t || t.Database != _db) throw new ArgumentException("not a transaction of this database", nameof(oldTransaction));
        t.Reinitialize(writeOptions, transactionOptions);
        return t;
    }

    /// <summary>The live transaction named <paramref name="name"/>, or null (GetTransactionByName).</summary>
    public Transaction? GetTransactionByName(string name)
    {
        lock (_nameMapLock) return _transactions.GetValueOrDefault(name);
    }

    /// <summary>
    /// The transactions in the prepared state (GetAllPreparedTransactions), including those recovered from the WAL at
    /// open. The caller decides to commit or roll them back.
    /// </summary>
    public List<Transaction> GetAllPreparedTransactions()
    {
        var list = new List<Transaction>();
        lock (_nameMapLock)
        {
            foreach (PessimisticTransaction t in _transactions.Values)
            {
                if (t.State == TransactionState.Prepared) list.Add(t);
            }
        }
        return list;
    }

    internal bool RegisterTransaction(PessimisticTransaction txn)
    {
        lock (_nameMapLock) return _transactions.TryAdd(txn.Name!, txn);
    }

    internal void UnregisterTransaction(PessimisticTransaction txn)
    {
        lock (_nameMapLock)
        {
            if (_transactions.TryGetValue(txn.Name!, out PessimisticTransaction? t) && t == txn) _transactions.Remove(txn.Name!);
        }
    }

    internal void InsertExpirableTransaction(ulong id, PessimisticTransaction txn)
    {
        lock (_expirableLock) _expirable[id] = txn;
    }

    internal void RemoveExpirableTransaction(ulong id)
    {
        lock (_expirableLock) _expirable.Remove(id);
    }

    /// <summary>TryStealingExpiredTransactionLocks: true if transaction <paramref name="id"/> is gone or expired and gave up its locks.</summary>
    internal bool TryStealingExpiredTransactionLocks(ulong id)
    {
        lock (_expirableLock)
        {
            if (!_expirable.TryGetValue(id, out PessimisticTransaction? t)) return true;
            return t.TryStealingLocks();
        }
    }

    /// <summary>Currently held key locks (GetLockStatusData).</summary>
    public List<KeyLockInfo> GetLockStatusData() => LockManager.GetLockStatusData();

    /// <summary>The most recently detected deadlocks, newest first (GetDeadlockInfoBuffer).</summary>
    public List<DeadlockPath> GetDeadlockInfoBuffer() => LockManager.GetDeadlockInfoBuffer();

    /// <summary>Changes how many deadlocks <see cref="GetDeadlockInfoBuffer"/> keeps (SetDeadlockInfoBufferSize).</summary>
    public void SetDeadlockInfoBufferSize(int size) => LockManager.Resize(size);

    // =====================================================================================================
    // Writes with concurrency control
    // =====================================================================================================

    private enum DirectOp : byte
    {
        Put,
        Delete,
        SingleDelete,
        Merge,
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Direct(null, null, DirectOp.Put, key, value);
    public void Put(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Direct(options, columnFamily, DirectOp.Put, key, value);
    public void Delete(ReadOnlySpan<byte> key) => Direct(null, null, DirectOp.Delete, key, default);
    public void Delete(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key) => Direct(options, columnFamily, DirectOp.Delete, key, default);
    public void SingleDelete(ReadOnlySpan<byte> key) => Direct(null, null, DirectOp.SingleDelete, key, default);
    public void SingleDelete(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key) => Direct(options, columnFamily, DirectOp.SingleDelete, key, default);
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Direct(null, null, DirectOp.Merge, key, operand);
    public void Merge(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Direct(options, columnFamily, DirectOp.Merge, key, operand);

    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        DirectAsync(null, null, DirectOp.Put, key, value, cancellationToken);
    public ValueTask PutAsync(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        DirectAsync(options, columnFamily, DirectOp.Put, key, value, cancellationToken);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        DirectAsync(null, null, DirectOp.Delete, key, default, cancellationToken);
    public ValueTask DeleteAsync(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        DirectAsync(options, columnFamily, DirectOp.Delete, key, default, cancellationToken);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        DirectAsync(null, null, DirectOp.Merge, key, operand, cancellationToken);
    public ValueTask MergeAsync(WriteOptions? options, ColumnFamily? columnFamily, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand,
        CancellationToken cancellationToken = default) =>
        DirectAsync(options, columnFamily, DirectOp.Merge, key, operand, cancellationToken);

    /// <summary>
    /// Writes <paramref name="batch"/> atomically after locking all of its keys (in sorted order, so batch writes
    /// cannot deadlock with each other) with the default lock timeout (WriteWithConcurrencyControl).
    /// </summary>
    public void Write(WriteOptions? options, WriteBatch batch)
    {
        ValueTask t = WriteAsync(options, batch, synchronous: true, CancellationToken.None);
        if (t.IsCompleted) t.GetAwaiter().GetResult();
        else t.AsTask().GetAwaiter().GetResult();
    }

    public ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, CancellationToken cancellationToken = default) =>
        WriteAsync(options, batch, synchronous: false, cancellationToken);

    private async ValueTask WriteAsync(WriteOptions? options, WriteBatch batch, bool synchronous, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.SkipConcurrencyControl)
        {
            await _db.Impl.WriteAsync(options, batch, validateColumnFamilies: true, synchronous, cancellationToken).ConfigureAwait(false);
            return;
        }
        using PessimisticTransaction txn = BeginInternalTransaction(options);
        await txn.CommitBatchAsync(batch, synchronous, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>BeginInternalTransaction: wraps a direct write; uses the default (not the transaction) lock timeout.</summary>
    private PessimisticTransaction BeginInternalTransaction(WriteOptions? options)
    {
        var txn = new PessimisticTransaction(this, options, new TransactionOptions());
        txn.SetLockTimeout(_options.DefaultLockTimeout);
        return txn;
    }

    private void Direct(WriteOptions? options, ColumnFamily? cf, DirectOp op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (_options.SkipConcurrencyControl)
        {
            using WriteBatch b = WriteBatch.Rent();
            AddOp(b, cf, op, key, value);
            _db.Impl.WriteAsync(options, b, validateColumnFamilies: true, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
            return;
        }
        // Since the client didn't create a transaction, they don't care about conflict checking for this write. So we
        // just need to do PutUntracked().
        using PessimisticTransaction txn = BeginInternalTransaction(options);
        switch (op)
        {
            case DirectOp.Put: if (cf is null) txn.PutUntracked(key, value); else txn.PutUntracked(cf, key, value); break;
            case DirectOp.Delete: if (cf is null) txn.DeleteUntracked(key); else txn.DeleteUntracked(cf, key); break;
            case DirectOp.SingleDelete: txn.SingleDeleteUntracked(cf, key); break;
            default: if (cf is null) txn.MergeUntracked(key, value); else txn.MergeUntracked(cf, key, value); break;
        }
        txn.Commit();
    }

    private async ValueTask DirectAsync(WriteOptions? options, ColumnFamily? cf, DirectOp op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken)
    {
        if (_options.SkipConcurrencyControl)
        {
            using WriteBatch b = WriteBatch.Rent();
            AddOp(b, cf, op, key.Span, value.Span);
            await _db.Impl.WriteAsync(options, b, validateColumnFamilies: true, synchronous: false, cancellationToken).ConfigureAwait(false);
            return;
        }
        using PessimisticTransaction txn = BeginInternalTransaction(options);
        await txn.WriteUntrackedAsync(cf, (byte)op, key, value, cancellationToken).ConfigureAwait(false);
        await txn.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddOp(WriteBatch b, ColumnFamily? cf, DirectOp op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        uint id = cf?.Id ?? 0;
        switch (op)
        {
            case DirectOp.Put: b.Put(id, key, value); break;
            case DirectOp.Delete: b.Delete(id, key); break;
            case DirectOp.SingleDelete: b.SingleDelete(id, key); break;
            default: b.Merge(id, key, value); break;
        }
    }

    // =====================================================================================================
    // Column families
    // =====================================================================================================

    /// <summary>Drops a column family and its lock table (DropColumnFamily).</summary>
    public void DropColumnFamily(ColumnFamily columnFamily)
    {
        _db.DropColumnFamily(columnFamily);
        LockManager.RemoveColumnFamily(columnFamily.Id);
    }

    // =====================================================================================================
    // Close
    // =====================================================================================================

    public void Dispose() => _db.Dispose();

    public ValueTask DisposeAsync() => _db.DisposeAsync();
}
