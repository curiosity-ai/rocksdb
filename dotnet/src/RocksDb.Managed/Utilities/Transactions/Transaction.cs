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
public abstract class Transaction : IDisposable
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
        Snapshot s = _database.GetSnapshot();
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

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(WriteOp.Put, Cfd(null), key, value, untracked: false);
    public void Put(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Write(WriteOp.Put, Cfd(columnFamily), key, value, untracked: false);
    public void Delete(ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(null), key, default, untracked: false);
    public void Delete(ColumnFamily columnFamily, ReadOnlySpan<byte> key) => Write(WriteOp.Delete, Cfd(columnFamily), key, default, untracked: false);
    public void SingleDelete(ReadOnlySpan<byte> key) => Write(WriteOp.SingleDelete, Cfd(null), key, default, untracked: false);
    public void SingleDelete(ColumnFamily columnFamily, ReadOnlySpan<byte> key) => Write(WriteOp.SingleDelete, Cfd(columnFamily), key, default, untracked: false);
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Write(WriteOp.Merge, Cfd(null), key, operand, untracked: false);
    public void Merge(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> operand) => Write(WriteOp.Merge, Cfd(columnFamily), key, operand, untracked: false);

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

    private void Write(WriteOp op, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool untracked)
    {
        ThrowIfDisposed();
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: false, exclusive: true, doValidate: !untracked, synchronous: true, CancellationToken.None)));
        Apply(op, cfd, key, value);
    }

    private ValueTask WriteAsync(WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) =>
        WriteAsync(op, cfd, key, value, untracked: false, cancellationToken);

    /// <summary>Untracked write with an async lock wait (used by <see cref="TransactionDatabase"/> direct writes).</summary>
    internal ValueTask WriteUntrackedAsync(ColumnFamily? columnFamily, byte op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken) =>
        WriteAsync((WriteOp)op, Cfd(columnFamily), key, value, untracked: true, cancellationToken);

    private ValueTask WriteAsync(WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, bool untracked, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ValueTask<RocksDbException?> t = TryLock(cfd, key.Span, readOnly: false, exclusive: true, doValidate: !untracked, synchronous: false, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            ThrowIfError(t.Result);
            Apply(op, cfd, key.Span, value.Span);
            return default;
        }
        return CompleteWriteAsync(t, op, cfd, key, value);
    }

    private async ValueTask CompleteWriteAsync(ValueTask<RocksDbException?> t, WriteOp op, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        ThrowIfError(await t.ConfigureAwait(false));
        Apply(op, cfd, key.Span, value.Span);
    }

    private void Apply(WriteOp op, ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        switch (op)
        {
            case WriteOp.Put: _batch.Put(cfd.Id, key, value); break;
            case WriteOp.Delete: _batch.Delete(cfd.Id, key); break;
            case WriteOp.SingleDelete: _batch.SingleDelete(cfd.Id, key); break;
            default: _batch.Merge(cfd.Id, key, value); break;
        }
    }

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

    public bool TryGetForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value, bool exclusive)
    {
        ThrowIfDisposed();
        ColumnFamilyData cfd = Cfd(columnFamily);
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: true, exclusive, doValidate: true, synchronous: true, CancellationToken.None)));
        return value is null ? false : GetFromBatchAndDb(options, cfd, key, value);
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
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(value);
        ColumnFamilyData cfd = Cfd(columnFamily);
        ValueTask<RocksDbException?> t = TryLock(cfd, key.Span, readOnly: true, exclusive, doValidate: true, synchronous: false, cancellationToken);
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
        return new TransactionIterator(handle.NewIterator(options), _batch, cfd, options);
    }

    private bool GetFromBatchAndDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
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
                    bool has = ColumnFamilyHandle(cfd).TryGet(options, key, scratch);
                    return _batch.ComputeValue(idx, cfd.MergeOperator, has, scratch.WrittenSpan, value);
                }
                finally
                {
                    ObjectPools.ReturnWriter(scratch);
                }
            }
            default:
                return ColumnFamilyHandle(cfd).TryGet(options, key, value);
        }
    }

    private ValueTask<bool> GetFromBatchAndDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken)
    {
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
                return ColumnFamilyHandle(cfd).TryGetAsync(options, key, value, cancellationToken);
        }
    }

    private async ValueTask<bool> MergeWithDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, IBufferWriter<byte> value,
        CancellationToken cancellationToken)
    {
        PooledBufferWriter scratch = ObjectPools.RentWriter();
        try
        {
            bool has = await ColumnFamilyHandle(cfd).TryGetAsync(options, key, scratch, cancellationToken).ConfigureAwait(false);
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
    public void SetSavePoint()
    {
        ThrowIfDisposed();
        (_savePoints ??= new List<SavePoint>()).Add(new SavePoint { Snapshot = _snapshot, SnapshotNeeded = _snapshotNeeded });
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

    /// <summary>Removes the most recent save point without rolling back (PopSavePoint). Returns false if there is none.</summary>
    public bool PopSavePoint()
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
    private protected abstract ValueTask<RocksDbException?> TryLock(ColumnFamilyData cfd, ReadOnlySpan<byte> key, bool readOnly, bool exclusive, bool doValidate,
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
    }

    private protected ColumnFamilyData Cfd(ColumnFamily? columnFamily)
    {
        if (columnFamily is null) return _database.DefaultColumnFamily.Data;
        if (columnFamily.Db != _db) ThrowHelper.InvalidArgument("column family does not belong to this database");
        return columnFamily.Data;
    }

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
