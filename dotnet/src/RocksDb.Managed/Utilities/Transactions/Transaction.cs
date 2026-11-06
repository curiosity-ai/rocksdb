using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// A transaction over a <see cref="TransactionDatabase"/> (pessimistic) or an <see cref="OptimisticTransactionDatabase"/>.
/// Port of rocksdb::Transaction / TransactionBaseImpl (include/rocksdb/utilities/transaction.h,
/// utilities/transactions/transaction_base.cc).
/// <para>
/// Writes are buffered in an indexed write batch and become visible to other readers only at commit. Reads through the
/// transaction (<see cref="TryGet(ReadOptions?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>, iterators) see the
/// transaction's own writes merged over the DB. Like upstream, reads use <see cref="ReadOptions.Snapshot"/> as given:
/// the transaction's snapshot (<see cref="SetSnapshot"/>) is used for conflict checking only, so pass
/// <c>new ReadOptions { Snapshot = txn.Snapshot }</c> for repeatable reads.
/// </para>
/// <para>
/// <see cref="TryGetForUpdate(ReadOptions?, ReadOnlySpan{byte}, IBufferWriter{byte})"/> and the write methods lock the
/// key (pessimistic: may wait up to the lock timeout; the async overloads wait without blocking a thread) or track it
/// for validation at commit (optimistic). Failures surface as <see cref="RocksDbException"/>:
/// <see cref="StatusCode.Busy"/> (write conflict; <see cref="StatusSubCode.Deadlock"/> for deadlocks),
/// <see cref="StatusCode.TimedOut"/> (lock wait timeout), <see cref="StatusCode.TryAgain"/> (optimistic validation
/// could not be done: not enough memtable history), <see cref="StatusCode.Expired"/>.
/// </para>
/// Not thread-safe: use a transaction from one logical flow at a time. Dispose it when done (an uncommitted
/// transaction releases its locks; a prepared one stays prepared in the WAL).
/// </summary>
public abstract partial class Transaction : IDisposable
{
    private protected readonly RocksDatabase _database;
    private protected readonly DbImpl _db;
    private protected WriteOptions _writeOptions;
    private protected readonly IndexedWriteBatch _batch;
    private protected readonly LockTracker _tracked = new();
    private protected readonly TransactionWriteContext _writeContext = new();
    private Snapshot? _snapshot;
    private bool _snapshotNeeded;
    private readonly List<Snapshot> _ownedSnapshots = new();
    private List<SavePoint>? _savePoints;
    private protected bool _disposed;

    private sealed class SavePoint
    {
        public Snapshot? Snapshot;
        public bool SnapshotNeeded;
        public long NumPuts, NumPutEntities, NumDeletes, NumMerges;
        public readonly LockTracker NewLocks = new();
    }

    private protected Transaction(RocksDatabase database, WriteOptions? writeOptions, int maxWriteBatchSize)
    {
        _database = database;
        _db = database.Impl;
        _writeOptions = writeOptions ?? new WriteOptions();
        // TransactionBaseImpl::InitWriteBatch: with allow_2pc the batch starts with a Noop that Prepare turns into the
        // begin-prepare marker.
        _batch = new IndexedWriteBatch(_db.Options.Allow2Pc, maxWriteBatchSize, _db.UserComparatorOf);
    }

    /// <summary>The database this transaction belongs to.</summary>
    public RocksDatabase Database => _database;

    /// <summary>The transaction's snapshot (set by <see cref="SetSnapshot"/>), or null.</summary>
    public Snapshot? Snapshot => _snapshot;

    /// <summary>Current state.</summary>
    public abstract TransactionState State { get; }

    /// <summary>Unique id of the transaction (pessimistic transactions; 0 for optimistic ones).</summary>
    public virtual ulong Id => 0;

    /// <summary>The name set by <see cref="SetName"/> (2PC), or null.</summary>
    public virtual string? Name => null;

    /// <summary>Number of operations (puts, deletes, merges) in the transaction's write batch.</summary>
    public int Count => _batch.Count;

    /// <summary>The write batch holding the transaction's writes. Do not modify it.</summary>
    public WriteBatch WriteBatch => _batch.Batch;

    private protected WriteBatch? _commitTimeBatch;

    /// <summary>
    /// The commit-time write batch (Transaction::GetCommitTimeWriteBatch): writes added to it are written by the commit
    /// of a prepared (2PC) pessimistic transaction, atomically with the commit marker, without concurrency control (no
    /// locks, no conflict checks), so use it with great caution. It is cleared when the transaction finishes.
    /// <list type="bullet">
    /// <item>WriteCommitted: the batch is written to the WAL with the commit marker and inserted into the memtables
    /// before the transaction's writes. Committing an unprepared transaction with a non-empty commit-time batch fails
    /// with InvalidArgument.</item>
    /// <item>WritePrepared / WriteUnprepared: only allowed with
    /// <see cref="TransactionOptions.UseOnlyTheLastCommitTimeBatchForRecovery"/> (otherwise the commit fails with
    /// InvalidArgument). The batch then only goes to the WAL; the DB keeps the last such batch and writes it to the
    /// memtables before the next memtable switch, so it is visible after a flush or a reopen (e.g. a replication
    /// position), not right after the commit.</item>
    /// <item>Optimistic transactions ignore it.</item>
    /// </list>
    /// </summary>
    public WriteBatch GetCommitTimeWriteBatch() => _commitTimeBatch ??= new WriteBatch();

    /// <summary>The write options used at commit.</summary>
    public WriteOptions WriteOptions
    {
        get => _writeOptions;
        set => _writeOptions = value ?? throw new ArgumentNullException(nameof(value));
    }

    // =====================================================================================================
    // Snapshots
    // =====================================================================================================

    /// <summary>
    /// Takes a snapshot now (Transaction::SetSnapshot): conflict checking then makes sure no other writer modified a
    /// key read or written by this transaction after this point.
    /// </summary>
    public void SetSnapshot()
    {
        ThrowIfDisposed();
        // TransactionBaseImpl::SetSnapshot: a write-conflict-boundary snapshot (WritePreparedTxn::SetSnapshot too).
        Snapshot s = _database.Impl.GetSnapshotForWriteConflictBoundary();
        _ownedSnapshots.Add(s);
        _snapshot = s;
        _snapshotNeeded = false;
    }

    /// <summary>Takes the snapshot lazily, at the next read or write through the transaction (SetSnapshotOnNextOperation).</summary>
    public void SetSnapshotOnNextOperation() => _snapshotNeeded = true;

    /// <summary>Clears the snapshot (ClearSnapshot): later operations are not checked against it.</summary>
    public void ClearSnapshot()
    {
        _snapshot = null;
        _snapshotNeeded = false;
    }

    private protected void SetSnapshotIfNeeded()
    {
        if (_snapshotNeeded) SetSnapshot();
    }

    // =====================================================================================================
    // Writes
    // =====================================================================================================

    private enum WriteOp : byte
    {
        Put,
        Delete,
        SingleDelete,
        Merge,
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(WriteOp.Put, Cfd(null), key, value, untracked: false, assumeTracked: false);
    public void Put(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        Write(WriteOp.Put, Cfd(columnFamily), key, value, untracked: false, assumeTracked: false);
    public void Delete(ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(null), key, default, untracked: false, assumeTracked: false);
    public void Delete(ColumnFamily columnFamily, ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(columnFamily), key, default, untracked: false, assumeTracked: false);
    public void SingleDelete(ReadOnlySpan<byte> key) => Write(WriteOp.SingleDelete, Cfd(null), key, default, untracked: false, assumeTracked: false);
    public void SingleDelete(ColumnFamily columnFamily, ReadOnlySpan<byte> key) =>
        Write(WriteOp.SingleDelete, Cfd(columnFamily), key, default, untracked: false, assumeTracked: false);
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Write(WriteOp.Merge, Cfd(null), key, operand, untracked: false, assumeTracked: false);
    public void Merge(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) =>
        Write(WriteOp.Merge, Cfd(columnFamily), key, operand, untracked: false, assumeTracked: false);

    /// <summary>
    /// Put with <c>assume_tracked</c> (TransactionBaseImpl::Put): when <paramref name="assumeTracked"/> is true the
    /// caller asserts that this transaction already locked and tracked the key exclusively since the last save point
    /// (e.g. with an exclusive <see cref="TryGetForUpdate(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, IBufferWriter{byte}, bool, bool)"/>),
    /// so the key is neither validated against the snapshot nor tracked again. A pessimistic transaction fails with
    /// InvalidArgument ("assume_tracked is set but it is not tracked yet") when the key is not tracked; an optimistic
    /// one does not support it (upstream asserts) and treats it like an untracked write.
    /// </summary>
    public void Put(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool assumeTracked) =>
        Write(WriteOp.Put, Cfd(columnFamily), key, value, untracked: false, assumeTracked);

    /// <summary>Merge with <c>assume_tracked</c> (see <see cref="Put(ColumnFamily, ReadOnlySpan{byte}, ReadOnlySpan{byte}, bool)"/>).</summary>
    public void Merge(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand, bool assumeTracked) =>
        Write(WriteOp.Merge, Cfd(columnFamily), key, operand, untracked: false, assumeTracked);

    /// <summary>Delete with <c>assume_tracked</c> (see <see cref="Put(ColumnFamily, ReadOnlySpan{byte}, ReadOnlySpan{byte}, bool)"/>).</summary>
    public void Delete(ColumnFamily columnFamily, ReadOnlySpan<byte> key, bool assumeTracked) =>
        Write(WriteOp.Delete, Cfd(columnFamily), key, default, untracked: false, assumeTracked);

    /// <summary>SingleDelete with <c>assume_tracked</c> (see <see cref="Put(ColumnFamily, ReadOnlySpan{byte}, ReadOnlySpan{byte}, bool)"/>).</summary>
    public void SingleDelete(ColumnFamily columnFamily, ReadOnlySpan<byte> key, bool assumeTracked) =>
        Write(WriteOp.SingleDelete, Cfd(columnFamily), key, default, untracked: false, assumeTracked);

    /// <summary>
    /// Put without conflict checking (PutUntracked): an optimistic transaction does not validate the key at commit; a
    /// pessimistic one still locks the key but does not check it against the snapshot.
    /// </summary>
    public void PutUntracked(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(WriteOp.Put, Cfd(null), key, value, untracked: true);
    public void PutUntracked(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(WriteOp.Put, Cfd(columnFamily), key, value, untracked: true);
    public void DeleteUntracked(ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(null), key, default, untracked: true);
    public void DeleteUntracked(ColumnFamily columnFamily, ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(columnFamily), key, default, untracked: true);
    public void SingleDeleteUntracked(ColumnFamily? columnFamily, ReadOnlySpan<byte> key) => Write(WriteOp.SingleDelete, Cfd(columnFamily), key, default, untracked: true);
    public void MergeUntracked(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Write(WriteOp.Merge, Cfd(null), key, operand, untracked: true);
    public void MergeUntracked(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Write(WriteOp.Merge, Cfd(columnFamily), key, operand, untracked: true);

    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Put, Cfd(null), key, value, cancellationToken);
    public ValueTask PutAsync(ColumnFamily columnFamily, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Put, Cfd(columnFamily), key, value, cancellationToken);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Delete, Cfd(null), key, default, cancellationToken);
    public ValueTask DeleteAsync(ColumnFamily columnFamily, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Delete, Cfd(columnFamily), key, default, cancellationToken);
    public ValueTask SingleDeleteAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.SingleDelete, Cfd(null), key, default, cancellationToken);
    public ValueTask SingleDeleteAsync(ColumnFamily columnFamily, ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.SingleDelete, Cfd(columnFamily), key, default, cancellationToken);
    public ValueTask MergeAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Merge, Cfd(null), key, operand, cancellationToken);
    public ValueTask MergeAsync(ColumnFamily columnFamily, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> operand, CancellationToken cancellationToken = default) =>
        WriteAsync(WriteOp.Merge, Cfd(columnFamily), key, operand, cancellationToken);

    /// <summary>Appends a blob to the WAL record of the commit, not applied to the memtables (PutLogData).</summary>
    public void PutLogData(ReadOnlySpan<byte> blob)
    {
        ThrowIfDisposed();
        _batch.PutLogData(blob);
    }

    private void Write(WriteOp op, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool untracked) =>
        Write(op, cfd, key, value, untracked, assumeTracked: false);

    /// <summary>TransactionBaseImpl::Put / Merge / Delete / SingleDelete (+ the Untracked forms): lock, then append.</summary>
    private void Write(WriteOp op, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool untracked, bool assumeTracked)
    {
        ThrowIfDisposed();
        BeforeWrite();
        // const bool do_validate = !assume_tracked;
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: false, exclusive: true, doValidate: !untracked && !assumeTracked, assumeTracked, synchronous: true,
            CancellationToken.None)));
        Apply(op, cfd, key, value);
        AfterWrite();
    }

    /// <summary>Called before a write locks its key (WriteUnpreparedTxn::HandleWrite flushes a large batch to the DB).</summary>
    private protected virtual void BeforeWrite()
    {
    }

    /// <summary>Called after a successful write (WriteUnpreparedTxn::HandleWrite tracks the largest validated sequence).</summary>
    private protected virtual void AfterWrite()
    {
    }

    private ValueTask WriteAsync(WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) =>
        WriteAsync(op, cfd, key, value, untracked: false, cancellationToken);

    /// <summary>Untracked write with an async lock wait (used by <see cref="TransactionDatabase"/> direct writes).</summary>
    internal ValueTask WriteUntrackedAsync(ColumnFamily? columnFamily, byte op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) =>
        WriteAsync((WriteOp)op, Cfd(columnFamily), key, value, untracked: true, cancellationToken);

    private ValueTask WriteAsync(WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, bool untracked, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        BeforeWrite();
        ValueTask<RocksDbException?> t = TryLock(cfd, key.Span, readOnly: false, exclusive: true, doValidate: !untracked, assumeTracked: false, synchronous: false,
            cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            ThrowIfError(t.Result);
            Apply(op, cfd, key.Span, value.Span);
            AfterWrite();
            return default;
        }
        return CompleteWriteAsync(t, op, cfd, key, value);
    }

    private async ValueTask CompleteWriteAsync(ValueTask<RocksDbException?> t, WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        ThrowIfError(await t.ConfigureAwait(false));
        Apply(op, cfd, key.Span, value.Span);
        AfterWrite();
    }

    private void Apply(WriteOp op, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        switch (op)
        {
            case WriteOp.Put: _batch.Put(cfd.Id, key, value); _numPuts++; break;
            case WriteOp.Delete: _batch.Delete(cfd.Id, key); _numDeletes++; break;
            case WriteOp.SingleDelete: _batch.SingleDelete(cfd.Id, key); _numDeletes++; break;
            default: _batch.Merge(cfd.Id, key, value); _numMerges++; break;
        }
    }

    // TransactionBaseImpl::num_puts_ / num_put_entities_ / num_deletes_ / num_merges_ (restored by RollbackToSavePoint).
    private protected long _numPuts, _numPutEntities, _numDeletes, _numMerges;
    // start_time_ (Stopwatch ticks), reset by Reinitialize.
    private protected long _startTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();

    /// <summary>GetNumPuts: Put operations applied to this transaction so far (incl. untracked ones).</summary>
    public long NumPuts => _numPuts;

    /// <summary>GetNumPutEntities: PutEntity operations applied so far.</summary>
    public long NumPutEntities => _numPutEntities;

    /// <summary>GetNumDeletes: Delete and SingleDelete operations applied so far.</summary>
    public long NumDeletes => _numDeletes;

    /// <summary>GetNumMerges: Merge operations applied so far.</summary>
    public long NumMerges => _numMerges;

    /// <summary>
    /// GetNumKeys: the number of distinct keys tracked by this transaction (pessimistic: the keys it holds locks on;
    /// optimistic: the keys checked for conflicts at commit).
    /// </summary>
    public long NumKeys => _tracked.NumPointLocks;

    /// <summary>GetElapsedTime: the time since the transaction began (milliseconds precision upstream).</summary>
    public TimeSpan ElapsedTime => System.Diagnostics.Stopwatch.GetElapsedTime(_startTimestamp);

    /// <summary>
    /// SetLockTimeout: changes this transaction's lock wait timeout (TransactionOptions.lock_timeout); a negative value
    /// waits forever. Has no effect on optimistic transactions.
    /// </summary>
    public virtual void SetLockTimeout(TimeSpan timeout)
    {
    }

    /// <summary>
    /// DisableIndexing: later Put / PutEntity / Merge / Delete operations are appended to the batch without being
    /// indexed, a performance optimization when the keys about to be written are not read back. Reading such a key
    /// through the transaction (Get, GetForUpdate, iterators) gives an undefined result, like upstream.
    /// </summary>
    public void DisableIndexing() => _batch.IndexingEnabled = false;

    /// <summary>EnableIndexing: later writes are indexed again (see <see cref="DisableIndexing"/>).</summary>
    public void EnableIndexing() => _batch.IndexingEnabled = true;

    /// <summary>Whether writes are currently indexed (indexing_enabled_).</summary>
    public bool IsIndexingEnabled => _batch.IndexingEnabled;

    // =====================================================================================================
    // Reads
    // =====================================================================================================

    /// <summary>
    /// Reads <paramref name="key"/> as this transaction sees it: its own writes (with merges applied) over the DB read
    /// with <paramref name="options"/> (GetFromBatchAndDB). Returns false if the key does not exist.
    /// </summary>
    public bool TryGet(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value) => TryGet(options, null!, key, value);

    public bool TryGet(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(value);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        return GetFromBatchAndDb(options, cfd, key, value);
    }

    /// <summary>Reads <paramref name="key"/> (see <see cref="TryGet(ReadOptions?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>); null if not found.</summary>
    public byte[]? Get(ReadOnlySpan<byte> key) => Get(null, null!, key);

    public byte[]? Get(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return TryGet(options, columnFamily, key, w) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    public ValueTask<bool> TryGetAsync(ReadOptions? options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        TryGetAsync(options, null!, key, value, cancellationToken);

    public ValueTask<bool> TryGetAsync(ReadOptions? options, ColumnFamily columnFamily, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(value);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        return GetFromBatchAndDbAsync(options, cfd, key, value, cancellationToken);
    }

    public async ValueTask<byte[]?> GetAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return await TryGetAsync(null, null!, key, w, cancellationToken).ConfigureAwait(false) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    /// <summary>
    /// Locks (pessimistic) or tracks (optimistic) <paramref name="key"/> for this transaction and then reads it
    /// (GetForUpdate). With a snapshot set, a pessimistic transaction fails with Busy if the key was written after the
    /// snapshot; an optimistic one fails at commit if the key was written after it was read.
    /// </summary>
    public bool TryGetForUpdate(ReadOptions? options, ReadOnlySpan<byte> key, IBufferWriter<byte> value) =>
        TryGetForUpdate(options, null!, key, value, exclusive: true);

    public bool TryGetForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value, bool exclusive) =>
        TryGetForUpdate(options, columnFamily, key, value, exclusive, doValidate: true);

    /// <summary>
    /// GetForUpdate with <c>do_validate</c>: when <paramref name="doValidate"/> is false the key is locked (pessimistic)
    /// but not validated against the transaction's snapshot or read timestamp, and an optimistic transaction does not
    /// track it for the commit-time conflict check. Without validation, a snapshot in <paramref name="options"/> or a
    /// read timestamp set with <see cref="SetReadTimestampForValidation"/> is an error (InvalidArgument), like upstream.
    /// </summary>
    public bool TryGetForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value, bool exclusive,
        bool doValidate)
    {
        ThrowIfDisposed();
        CheckDoValidate(options, doValidate);
        ColumnFamilyData cfd = Cfd(columnFamily);
        options = PrepareGetForUpdateOptions(options, cfd, doValidate);
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: true, exclusive, doValidate, assumeTracked: false, synchronous: true, CancellationToken.None)));
        return value is null ? false : GetFromBatchAndDb(options, cfd, key, value);
    }

    /// <summary>TransactionBaseImpl::GetForUpdate: do_validate = false is not defined with a snapshot.</summary>
    private static void CheckDoValidate(ReadOptions? options, bool doValidate)
    {
        if (!doValidate && options?.Snapshot is not null)
        {
            ThrowHelper.InvalidArgument("If do_validate is false then GetForUpdate with snapshot is not defined.");
        }
    }

    /// <summary>GetForUpdate with <c>do_validate</c> (see <see cref="TryGetForUpdate(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, IBufferWriter{byte}, bool, bool)"/>).</summary>
    public byte[]? GetForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, bool exclusive, bool doValidate)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return TryGetForUpdate(options, columnFamily, key, w, exclusive, doValidate) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    public byte[]? GetForUpdate(ReadOnlySpan<byte> key) => GetForUpdate(null, null!, key, exclusive: true);

    public byte[]? GetForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, bool exclusive)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return TryGetForUpdate(options, columnFamily, key, w, exclusive) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    public ValueTask<bool> TryGetForUpdateAsync(ReadOptions? options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, CancellationToken cancellationToken = default) =>
        TryGetForUpdateAsync(options, null!, key, value, exclusive: true, cancellationToken);

    public ValueTask<bool> TryGetForUpdateAsync(ReadOptions? options, ColumnFamily columnFamily, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, bool exclusive,
        CancellationToken cancellationToken = default) =>
        TryGetForUpdateAsync(options, columnFamily, key, value, exclusive, doValidate: true, cancellationToken);

    /// <summary>Async GetForUpdate with <c>do_validate</c> (see <see cref="TryGetForUpdate(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, IBufferWriter{byte}, bool, bool)"/>).</summary>
    public ValueTask<bool> TryGetForUpdateAsync(ReadOptions? options, ColumnFamily columnFamily, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, bool exclusive,
        bool doValidate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(value);
        CheckDoValidate(options, doValidate);
        ColumnFamilyData cfd = Cfd(columnFamily);
        options = PrepareGetForUpdateOptions(options, cfd, doValidate);
        ValueTask<RocksDbException?> t = TryLock(cfd, key.Span, readOnly: true, exclusive, doValidate, assumeTracked: false, synchronous: false, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            ThrowIfError(t.Result);
            return GetFromBatchAndDbAsync(options, cfd, key, value, cancellationToken);
        }
        return CompleteGetForUpdateAsync(t, options, cfd, key, value, cancellationToken);
    }

    private async ValueTask<bool> CompleteGetForUpdateAsync(ValueTask<RocksDbException?> t, ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key,
        IBufferWriter<byte> value, CancellationToken cancellationToken)
    {
        ThrowIfError(await t.ConfigureAwait(false));
        return await GetFromBatchAndDbAsync(options, cfd, key, value, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<byte[]?> GetForUpdateAsync(ReadOnlyMemory<byte> key, CancellationToken cancellationToken = default)
    {
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            return await TryGetForUpdateAsync(null, null!, key, w, exclusive: true, cancellationToken).ConfigureAwait(false) ? w.WrittenSpan.ToArray() : null;
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    // ---- range locks ----------------------------------------------------------------------------------------

    /// <summary>
    /// Locks the range [<paramref name="start"/>, <paramref name="end"/>] of <paramref name="columnFamily"/> (null =
    /// default) exclusively until the transaction ends (Transaction::GetRangeLock). Only a pessimistic transaction of a
    /// <see cref="TransactionDatabase"/> opened with a <see cref="TransactionDbOptions.RangeLockManager"/> supports it
    /// (NotSupported otherwise). A conflicting lock of another transaction is waited for up to the lock timeout
    /// (TimedOut / LockTimeout); a wait that would close a cycle fails with Busy / Deadlock. Writes and GetForUpdate of
    /// keys inside a range this transaction locked are granted at once. <paramref name="start"/> must not be greater
    /// than <paramref name="end"/>.
    /// </summary>
    public void GetRangeLock(ColumnFamily? columnFamily, RangeLockEndpoint start, RangeLockEndpoint end)
    {
        ThrowIfDisposed();
        ThrowIfError(WaitSync(TryRangeLock(Cfd(columnFamily), start, end, synchronous: true, CancellationToken.None)));
    }

    /// <summary>Async <see cref="GetRangeLock(ColumnFamily?, RangeLockEndpoint, RangeLockEndpoint)"/>: lock waits do not block a thread.</summary>
    public async ValueTask GetRangeLockAsync(ColumnFamily? columnFamily, RangeLockEndpoint start, RangeLockEndpoint end, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ThrowIfError(await TryRangeLock(Cfd(columnFamily), start, end, synchronous: false, cancellationToken).ConfigureAwait(false));
    }

    /// <summary>Range lock (PessimisticTransaction::GetRangeLock); NotSupported by default.</summary>
    private protected virtual ValueTask<RocksDbException?> TryRangeLock(ColumnFamilyData cfd, RangeLockEndpoint start, RangeLockEndpoint end, bool synchronous,
        CancellationToken cancellationToken) =>
        new(new RocksDbException(StatusCode.NotSupported, "Not implemented"));

    /// <summary>
    /// Undoes one <see cref="TryGetForUpdate(ReadOptions?, ReadOnlySpan{byte}, IBufferWriter{byte})"/> of the key
    /// (UndoGetForUpdate): once no read or write of this transaction (since the last save point) remains, the key is
    /// no longer checked for conflicts and its lock is released.
    /// </summary>
    public void UndoGetForUpdate(ReadOnlySpan<byte> key) => UndoGetForUpdate(null!, key);

    public void UndoGetForUpdate(ColumnFamily columnFamily, ReadOnlySpan<byte> key)
    {
        ThrowIfDisposed();
        ColumnFamilyData cfd = Cfd(columnFamily);
        bool canUntrack;
        if (_savePoints is { Count: > 0 } sps)
        {
            // If there is no GetForUpdate of the key in this save point, then cannot untrack from the global lock tracker.
            canUntrack = sps[^1].NewLocks.UntrackRead(cfd.Id, key) != UntrackStatus.NotTracked;
        }
        else
        {
            canUntrack = true;
        }
        if (canUntrack && _tracked.UntrackRead(cfd.Id, key) == UntrackStatus.Removed) UnlockGetForUpdate(cfd, key);
    }

    /// <summary>Iterator over the DB (read with <paramref name="options"/>) merged with this transaction's writes (GetIterator).</summary>
    public TransactionIterator NewIterator(ReadOptions? options) => NewIterator(null!, options);

    public TransactionIterator NewIterator(ColumnFamily columnFamily, ReadOptions? options)
    {
        ThrowIfDisposed();
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        ColumnFamily handle = columnFamily ?? _database.DefaultColumnFamily;
        return new TransactionIterator(NewDbIterator(handle, cfd, options), _batch, cfd, options);
    }

    /// <summary>
    /// Looks up every key of <paramref name="batch"/> as this transaction sees it (Transaction::MultiGet ->
    /// WriteBatchWithIndex::MultiGetFromBatchAndDB): keys the transaction's writes do not settle are read from the DB
    /// with one MultiGet (one consistent view), then the transaction's merges are applied. Read the results with
    /// <see cref="MultiGetBatch.TryGetValue"/>, <see cref="MultiGetBatch.GetStatus"/> and <see cref="MultiGetBatch.GetError"/>;
    /// a key's failure (e.g. a merge error) does not fail the others. Keys are not locked (see <see cref="MultiGetForUpdate"/>).
    /// </summary>
    public void MultiGet(ReadOptions? options, ColumnFamily columnFamily, MultiGetBatch batch)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        var reader = new TxnMultiValueReader(this, options, cfd);
        _batch.MultiGet(cfd.Id, cfd.MergeOperator, cfd.TimestampSize, options, batch, ref reader);
    }

    /// <summary><see cref="MultiGet(ReadOptions?, ColumnFamily, MultiGetBatch)"/> on the default column family.</summary>
    public void MultiGet(ReadOptions? options, MultiGetBatch batch) => MultiGet(options, null!, batch);

    /// <summary>
    /// TransactionBaseImpl::MultiGetForUpdate: locks (pessimistic, exclusively) or tracks (optimistic) every key of
    /// <paramref name="batch"/> like <see cref="TryGetForUpdate(ReadOptions?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>,
    /// then reads them like <see cref="MultiGet(ReadOptions?, ColumnFamily, MultiGetBatch)"/>. If a key cannot be locked
    /// (Busy, TimedOut, ...), the whole call fails with that error (upstream returns it for every key); keys locked
    /// before it stay locked, like upstream.
    /// </summary>
    public void MultiGetForUpdate(ReadOptions? options, ColumnFamily columnFamily, MultiGetBatch batch)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ColumnFamilyData cfd = Cfd(columnFamily);
        options = PrepareGetForUpdateOptions(options, cfd, doValidate: true);
        // Lock all keys; fail entire multiget if we cannot lock all keys.
        for (int i = 0; i < batch.Count; i++)
        {
            ThrowIfError(WaitSync(TryLock(cfd, batch.GetKey(i), readOnly: true, exclusive: true, doValidate: true, assumeTracked: false, synchronous: true,
                CancellationToken.None)));
        }
        SetSnapshotIfNeeded();
        var reader = new TxnMultiValueReader(this, options, cfd);
        _batch.MultiGet(cfd.Id, cfd.MergeOperator, cfd.TimestampSize, options, batch, ref reader);
    }

    /// <summary><see cref="MultiGetForUpdate(ReadOptions?, ColumnFamily, MultiGetBatch)"/> on the default column family.</summary>
    public void MultiGetForUpdate(ReadOptions? options, MultiGetBatch batch) => MultiGetForUpdate(options, null!, batch);

    /// <summary>The DB part of MultiGetFromBatchAndDB: one MultiGet of <paramref name="keys"/>.</summary>
    private protected virtual void MultiGetFromDb(ReadOptions? options, ColumnFamilyData cfd, MultiGetBatch keys) =>
        ColumnFamilyHandle(cfd).MultiGet(options, keys);

    private readonly struct TxnMultiValueReader(Transaction txn, ReadOptions? options, ColumnFamilyData cfd) : WriteBatchWithIndex.IMultiValueDbReader
    {
        public void MultiGet(MultiGetBatch keys) => txn.MultiGetFromDb(options, cfd, keys);
    }

    private bool GetFromBatchAndDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        RequireReadTimestamp(options, cfd);
        int idx = _batch.Find(cfd.Id, key);
        switch (_batch.Classify(idx))
        {
            case BatchLookupResult.Found:
                return _batch.ComputeValue(idx, cfd.MergeOperator, false, default, value);
            case BatchLookupResult.Deleted:
                return false;
            case BatchLookupResult.MergeInProgress:
            {
                PooledBufferWriter scratch = ObjectPools.RentWriter();
                try
                {
                    bool has = GetFromDb(options, cfd, key, scratch);
                    return _batch.ComputeValue(idx, cfd.MergeOperator, has, scratch.WrittenSpan, value);
                }
                finally
                {
                    ObjectPools.ReturnWriter(scratch);
                }
            }
            default:
                return GetFromDb(options, cfd, key, value);
        }
    }

    /// <summary>
    /// The DB part of GetFromBatchAndDB. WriteUnprepared transactions read with a read callback that also sees their
    /// own unprepared writes.
    /// </summary>
    private protected virtual bool GetFromDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, IBufferWriter<byte> value) =>
        ColumnFamilyHandle(cfd).TryGet(options, key, value);

    /// <summary>Async <see cref="GetFromDb"/>.</summary>
    private protected virtual ValueTask<bool> GetFromDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken) =>
        ColumnFamilyHandle(cfd).TryGetAsync(options, key, value, cancellationToken);

    /// <summary>The DB iterator of <see cref="NewIterator(ColumnFamily, ReadOptions?)"/> (GetIterator's db_iter).</summary>
    private protected virtual Iterator NewDbIterator(ColumnFamily handle, ColumnFamilyData cfd, ReadOptions? options) => handle.NewIterator(options);

    /// <summary>WriteBatchWithIndex::GetFromBatchAndDB: reads of a column family with timestamps need a read timestamp.</summary>
    private static void RequireReadTimestamp(ReadOptions? options, ColumnFamilyData cfd)
    {
        if (cfd.TimestampSize > 0 && options?.Timestamp is null) ThrowHelper.InvalidArgument("Must specify timestamp");
    }

    private ValueTask<bool> GetFromBatchAndDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken)
    {
        RequireReadTimestamp(options, cfd);
        int idx = _batch.Find(cfd.Id, key.Span);
        switch (_batch.Classify(idx))
        {
            case BatchLookupResult.Found:
                return new ValueTask<bool>(_batch.ComputeValue(idx, cfd.MergeOperator, false, default, value));
            case BatchLookupResult.Deleted:
                return new ValueTask<bool>(false);
            case BatchLookupResult.MergeInProgress:
                return MergeWithDbAsync(options, cfd, key, value, cancellationToken);
            default:
                return GetFromDbAsync(options, cfd, key, value, cancellationToken);
        }
    }

    private async ValueTask<bool> MergeWithDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken)
    {
        PooledBufferWriter scratch = ObjectPools.RentWriter();
        try
        {
            bool has = await GetFromDbAsync(options, cfd, key, scratch, cancellationToken).ConfigureAwait(false);
            // Re-resolve: the batch is not modified concurrently (transactions are single-flow), but the index may move.
            int idx = _batch.Find(cfd.Id, key.Span);
            if (idx < 0) return has && Copy(scratch.WrittenSpan, value);
            return _batch.ComputeValue(idx, cfd.MergeOperator, has, scratch.WrittenSpan, value);
        }
        finally
        {
            ObjectPools.ReturnWriter(scratch);
        }
    }

    private static bool Copy(ReadOnlySpan<byte> src, IBufferWriter<byte> dst)
    {
        dst.Write(src);
        return true;
    }

    private ColumnFamily ColumnFamilyHandle(ColumnFamilyData cfd) => cfd.Handle ?? _database.GetColumnFamily(cfd.Name);

    // =====================================================================================================
    // Save points
    // =====================================================================================================

    /// <summary>Records the current state; <see cref="RollbackToSavePoint"/> undoes everything done after it (SetSavePoint).</summary>
    public virtual void SetSavePoint()
    {
        ThrowIfDisposed();
        (_savePoints ??= new List<SavePoint>()).Add(new SavePoint
        {
            Snapshot = _snapshot,
            SnapshotNeeded = _snapshotNeeded,
            NumPuts = _numPuts,
            NumPutEntities = _numPutEntities,
            NumDeletes = _numDeletes,
            NumMerges = _numMerges,
        });
        _batch.SetSavePoint();
    }

    /// <summary>
    /// Undoes the writes, snapshot changes and key tracking done since the most recent save point and removes it
    /// (RollbackToSavePoint). Returns false if there is no save point (upstream NotFound).
    /// </summary>
    public virtual bool RollbackToSavePoint()
    {
        ThrowIfDisposed();
        if (_savePoints is not { Count: > 0 } sps) return false;
        SavePoint sp = sps[^1];
        _snapshot = sp.Snapshot;
        _snapshotNeeded = sp.SnapshotNeeded;
        _numPuts = sp.NumPuts;
        _numPutEntities = sp.NumPutEntities;
        _numDeletes = sp.NumDeletes;
        _numMerges = sp.NumMerges;
        // Rollback batch
        _batch.RollbackToSavePoint();
        // Rollback any keys that were tracked since the last savepoint
        List<(ColumnFamilyData Cfd, byte[] Key)>? untracked = NeedsUnlockOnUntrack ? new() : null;
        _tracked.Subtract(sp.NewLocks, untracked);
        sps.RemoveAt(sps.Count - 1);
        if (untracked is not null)
        {
            foreach ((ColumnFamilyData cfd, byte[] key) in untracked) UnlockGetForUpdate(cfd, key);
        }
        return true;
    }

    /// <summary>Number of save points (save_points_ size).</summary>
    private protected int SavePointCount => _savePoints?.Count ?? 0;

    /// <summary>The keys tracked since the most recent save point (save_points_->top().new_locks_). REQUIRES: a save point.</summary>
    private protected LockTracker TopSavePointLocks => _savePoints![^1].NewLocks;

    /// <summary>Removes the most recent save point without rolling back (PopSavePoint). Returns false if there is none.</summary>
    public virtual bool PopSavePoint()
    {
        ThrowIfDisposed();
        if (_savePoints is not { Count: > 0 } sps) return false;
        SavePoint top = sps[^1];
        sps.RemoveAt(sps.Count - 1);
        // If there is another savepoint A below the current savepoint B, then A needs to inherit tracked_keys in B so
        // that if we rollback to savepoint A, we remember to unlock keys in B.
        if (sps.Count > 0) sps[^1].NewLocks.MergeFrom(top.NewLocks);
        _batch.PopSavePoint();
        return true;
    }

    // =====================================================================================================
    // Commit / rollback / 2PC
    // =====================================================================================================

    /// <summary>Commits the transaction: its writes become visible atomically (Commit). Throws on conflicts.</summary>
    public void Commit()
    {
        ValueTask t = CommitCoreAsync(synchronous: true, CancellationToken.None);
        if (t.IsCompleted) t.GetAwaiter().GetResult();
        else t.AsTask().GetAwaiter().GetResult();
    }

    /// <summary>Commits the transaction asynchronously (the WAL write and any lock-free validation do not block a thread).</summary>
    public ValueTask CommitAsync(CancellationToken cancellationToken = default) => CommitCoreAsync(synchronous: false, cancellationToken);

    /// <summary>Discards the transaction's writes and releases its locks (Rollback). A prepared transaction writes a rollback marker.</summary>
    public void Rollback()
    {
        ValueTask t = RollbackCoreAsync(synchronous: true, CancellationToken.None);
        if (t.IsCompleted) t.GetAwaiter().GetResult();
        else t.AsTask().GetAwaiter().GetResult();
    }

    public ValueTask RollbackAsync(CancellationToken cancellationToken = default) => RollbackCoreAsync(synchronous: false, cancellationToken);

    /// <summary>
    /// First phase of a two-phase commit (Prepare): writes the transaction's batch to the WAL as a prepared section
    /// named <see cref="Name"/>, without applying it. After a crash the transaction is recovered in the prepared state
    /// (<see cref="TransactionDatabase.GetAllPreparedTransactions"/>). Only pessimistic transactions support it.
    /// </summary>
    public void Prepare()
    {
        ValueTask t = PrepareCoreAsync(synchronous: true, CancellationToken.None);
        if (t.IsCompleted) t.GetAwaiter().GetResult();
        else t.AsTask().GetAwaiter().GetResult();
    }

    public ValueTask PrepareAsync(CancellationToken cancellationToken = default) => PrepareCoreAsync(synchronous: false, cancellationToken);

    /// <summary>Names the transaction (required before <see cref="Prepare"/>), 1..512 characters (SetName).</summary>
    public virtual void SetName(string name) =>
        throw new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Optimistic transactions cannot be named.");

    private protected abstract ValueTask CommitCoreAsync(bool synchronous, CancellationToken cancellationToken);
    private protected abstract ValueTask RollbackCoreAsync(bool synchronous, CancellationToken cancellationToken);

    private protected virtual ValueTask PrepareCoreAsync(bool synchronous, CancellationToken cancellationToken) =>
        ValueTask.FromException(new RocksDbException(StatusCode.InvalidArgument, "Invalid argument: Two phase commit not supported for optimistic transactions."));

    // =====================================================================================================
    // Subclass contract
    // =====================================================================================================

    /// <summary>
    /// Locks or tracks a key (TransactionBaseImpl::TryLock). Completes with null on success or the error. Must not
    /// keep <paramref name="key"/> beyond the synchronous part of the call.
    /// </summary>
    private protected abstract ValueTask<RocksDbException?> TryLock(ColumnFamilyData cfd, ReadOnlySpan<byte> key, bool readOnly, bool exclusive, bool doValidate, bool assumeTracked,
        bool synchronous, CancellationToken cancellationToken);

    /// <summary>Releases the lock of a key that is no longer tracked (pessimistic only).</summary>
    private protected virtual void UnlockGetForUpdate(ColumnFamilyData cfd, ReadOnlySpan<byte> key) { }

    /// <summary>True if untracked keys must be unlocked (pessimistic).</summary>
    private protected virtual bool NeedsUnlockOnUntrack => false;

    /// <summary>TransactionBaseImpl::TrackKey: tracks the key globally and in the current save point.</summary>
    private protected void TrackKey(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ulong seq, bool readOnly, bool exclusive)
    {
        _tracked.Track(cfd, key, seq, readOnly, exclusive);
        if (_savePoints is { Count: > 0 } sps) sps[^1].NewLocks.Track(cfd, key, seq, readOnly, exclusive);
    }

    /// <summary>TransactionBaseImpl::Clear: drops the writes, tracked keys and save points (snapshots are kept).</summary>
    private protected virtual void Clear()
    {
        _savePoints?.Clear();
        _batch.Clear();
        _tracked.Clear();
        _commitTimeBatch?.Clear();
        _numPuts = _numPutEntities = _numDeletes = _numMerges = 0;
    }

    private protected ColumnFamilyData Cfd(ColumnFamily? columnFamily)
    {
        ColumnFamilyData cfd;
        if (columnFamily is null)
        {
            cfd = _database.DefaultColumnFamily.Data;
        }
        else
        {
            if (columnFamily.Db != _db) ThrowHelper.InvalidArgument("column family does not belong to this database");
            cfd = columnFamily.Data;
        }
        // Only WriteCommitted pessimistic transactions support user-defined timestamps (as upstream).
        if (cfd.TimestampSize != 0 && !SupportsUserDefinedTimestamps) ThrowHelper.NotSupported("transactions on column families with user-defined timestamps");
        return cfd;
    }

    /// <summary>kMaxTxnTimestamp: "no timestamp set".</summary>
    public const ulong MaxTimestamp = ulong.MaxValue;

    /// <summary>Whether this kind of transaction supports column families with user-defined timestamps.</summary>
    private protected virtual bool SupportsUserDefinedTimestamps => false;

    /// <summary>
    /// Transaction::SetReadTimestampForValidation: keys locked with validation (GetForUpdate, writes) on column families
    /// with user-defined timestamps must not have a version newer than <paramref name="timestamp"/>. Cannot decrease.
    /// Only supported by pessimistic (WriteCommitted) transactions.
    /// </summary>
    public virtual void SetReadTimestampForValidation(ulong timestamp) => ThrowHelper.NotSupported("timestamp not supported");

    /// <summary>
    /// Transaction::SetCommitTimestamp: the timestamp assigned at commit to every write of this transaction on column
    /// families with user-defined timestamps (required when there are such writes). Only supported by pessimistic
    /// (WriteCommitted) transactions.
    /// </summary>
    public virtual void SetCommitTimestamp(ulong timestamp) => ThrowHelper.NotSupported("timestamp not supported");

    /// <summary>The commit timestamp (<see cref="MaxTimestamp"/> when not set).</summary>
    public virtual ulong CommitTimestamp => MaxTimestamp;

    /// <summary>
    /// GetForUpdate's read options on a column family with user-defined timestamps (WriteCommittedTxn::GetForUpdateImpl):
    /// the read timestamp defaults to the transaction's read timestamp for validation and must equal it otherwise.
    /// </summary>
    private protected virtual ReadOptions? PrepareGetForUpdateOptions(ReadOptions? options, ColumnFamilyData cfd, bool doValidate) => options;

    private protected static RocksDbException? WaitSync(ValueTask<RocksDbException?> t) =>
        t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private protected static void ThrowIfError(RocksDbException? e)
    {
        if (e is not null) Throw(e);
    }

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    private static void Throw(RocksDbException e) => throw e;

    private protected void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private protected void ReleaseSnapshots()
    {
        foreach (Snapshot s in _ownedSnapshots) s.Dispose();
        _ownedSnapshots.Clear();
        _snapshot = null;
        _snapshotNeeded = false;
    }

    /// <summary>Releases the transaction. Uncommitted writes are discarded and locks released (a prepared transaction stays prepared in the WAL).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        DisposeCore();
        ReleaseSnapshots();
        _batch.Dispose();
        GC.SuppressFinalize(this);
    }

    private protected virtual void DisposeCore() { }
}
