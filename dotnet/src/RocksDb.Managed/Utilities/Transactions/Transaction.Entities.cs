using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Wide-column entities in transactions (include/rocksdb/utilities/transaction.h; TransactionBaseImpl::PutEntity /
/// GetEntity / MultiGetEntity / GetEntityForUpdate / GetCoalescingIterator / GetAttributeGroupIterator,
/// utilities/transactions/transaction_base.cc, WriteCommittedTxn::GetEntityForUpdate in pessimistic_transaction.cc).
/// Reads see the transaction's own writes over the DB (WriteBatchWithIndex::GetEntityFromBatchAndDB): a plain value
/// reads as a single default column; merge operands apply to the default column and keep the other columns.
/// </summary>
public abstract partial class Transaction
{
    // ---- writes ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Writes a wide-column entity for <paramref name="key"/> in this transaction (PutEntity), locking (pessimistic) or
    /// tracking (optimistic) the key like <see cref="Put(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>. Columns may be given
    /// in any order; names must be unique. Column families with user-defined timestamps are not supported (NotSupported).
    /// </summary>
    public void PutEntity(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => WriteEntity(Cfd(null), key, columns, untracked: false);

    public void PutEntity(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) =>
        WriteEntity(Cfd(columnFamily), key, columns, untracked: false);

    /// <summary>PutEntityUntracked: like <see cref="PutEntity(ReadOnlySpan{byte}, ReadOnlySpan{WideColumn})"/> without conflict checking of the key.</summary>
    public void PutEntityUntracked(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => WriteEntity(Cfd(null), key, columns, untracked: true);

    public void PutEntityUntracked(ColumnFamily columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) =>
        WriteEntity(Cfd(columnFamily), key, columns, untracked: true);

    /// <summary>Async <see cref="PutEntity(ReadOnlySpan{byte}, ReadOnlySpan{WideColumn})"/>: a lock wait does not block a thread.</summary>
    public ValueTask PutEntityAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns, CancellationToken cancellationToken = default) =>
        WriteEntityAsync(Cfd(null), key, columns, cancellationToken);

    public ValueTask PutEntityAsync(ColumnFamily columnFamily, ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns,
        CancellationToken cancellationToken = default) =>
        WriteEntityAsync(Cfd(columnFamily), key, columns, cancellationToken);

    private void WriteEntity(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns, bool untracked)
    {
        ThrowIfDisposed();
        BeforeWrite();
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: false, exclusive: true, doValidate: !untracked, assumeTracked: false, synchronous: true, CancellationToken.None)));
        _batch.PutEntity(cfd.Id, key, columns);
        _numPutEntities++;
        AfterWrite();
    }

    private ValueTask WriteEntityAsync(ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        BeforeWrite();
        ValueTask<RocksDbException?> t = TryLock(cfd, key.Span, readOnly: false, exclusive: true, doValidate: true, assumeTracked: false, synchronous: false, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            ThrowIfError(t.Result);
            _batch.PutEntity(cfd.Id, key.Span, columns.Span);
            _numPutEntities++;
            AfterWrite();
            return default;
        }
        return CompleteWriteEntityAsync(t, cfd, key, columns);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask CompleteWriteEntityAsync(ValueTask<RocksDbException?> t, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, ReadOnlyMemory<WideColumn> columns)
    {
        ThrowIfError(await t.ConfigureAwait(false));
        _batch.PutEntity(cfd.Id, key.Span, columns.Span);
        _numPutEntities++;
        AfterWrite();
    }

    // ---- reads ----------------------------------------------------------------------------------------------

    /// <summary>
    /// Reads the entity of <paramref name="key"/> as this transaction sees it (GetEntity): its own writes over the DB
    /// read with <paramref name="options"/>. Returns false (columns emptied) if the key does not exist. A column family
    /// with user-defined timestamps requires <see cref="ReadOptions.Timestamp"/>; one without rejects it.
    /// </summary>
    public bool TryGetEntity(ReadOptions? options, ReadOnlySpan<byte> key, WideColumns columns) => TryGetEntity(options, null!, key, columns);

    public bool TryGetEntity(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, WideColumns columns)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(columns);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        return GetEntityFromBatchAndDb(options, cfd, key, columns);
    }

    /// <summary>The entity of <paramref name="key"/> in the default column family, or null (allocates; see <see cref="TryGetEntity(ReadOptions?, ReadOnlySpan{byte}, WideColumns)"/>).</summary>
    public WideColumns? GetEntity(ReadOnlySpan<byte> key)
    {
        var c = new WideColumns();
        return TryGetEntity(null, key, c) ? c : null;
    }

    /// <summary>Async <see cref="TryGetEntity(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, WideColumns)"/>: a key the transaction did not write is read from the DB with async I/O.</summary>
    public ValueTask<bool> TryGetEntityAsync(ReadOptions? options, ColumnFamily columnFamily, ReadOnlyMemory<byte> key, WideColumns columns,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(columns);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        return GetEntityFromBatchAndDbAsync(options, cfd, key, columns, cancellationToken);
    }

    public ValueTask<bool> TryGetEntityAsync(ReadOptions? options, ReadOnlyMemory<byte> key, WideColumns columns, CancellationToken cancellationToken = default) =>
        TryGetEntityAsync(options, null!, key, columns, cancellationToken);

    /// <summary>
    /// Locks (pessimistic) or tracks (optimistic) <paramref name="key"/> and reads its entity (GetEntityForUpdate);
    /// same locking, validation and timestamp rules as
    /// <see cref="TryGetForUpdate(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, System.Buffers.IBufferWriter{byte}, bool, bool)"/>.
    /// </summary>
    public bool TryGetEntityForUpdate(ReadOptions? options, ColumnFamily columnFamily, ReadOnlySpan<byte> key, WideColumns columns, bool exclusive,
        bool doValidate)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(columns);
        CheckEntityDoValidate(options, doValidate);
        ColumnFamilyData cfd = Cfd(columnFamily);
        options = PrepareGetForUpdateOptions(options, cfd, doValidate);
        ThrowIfError(WaitSync(TryLock(cfd, key, readOnly: true, exclusive, doValidate, assumeTracked: false, synchronous: true, CancellationToken.None)));
        return GetEntityFromBatchAndDb(options, cfd, key, columns);
    }

    public bool TryGetEntityForUpdate(ReadOptions? options, ReadOnlySpan<byte> key, WideColumns columns) =>
        TryGetEntityForUpdate(options, null!, key, columns, exclusive: true, doValidate: true);

    /// <summary>Async <see cref="TryGetEntityForUpdate(ReadOptions?, ColumnFamily, ReadOnlySpan{byte}, WideColumns, bool, bool)"/>: a lock wait does not block a thread.</summary>
    public async ValueTask<bool> TryGetEntityForUpdateAsync(ReadOptions? options, ColumnFamily columnFamily, ReadOnlyMemory<byte> key, WideColumns columns,
        bool exclusive, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(columns);
        ColumnFamilyData cfd = Cfd(columnFamily);
        options = PrepareGetForUpdateOptions(options, cfd, doValidate: true);
        ThrowIfError(await TryLock(cfd, key.Span, readOnly: true, exclusive, doValidate: true, assumeTracked: false, synchronous: false, cancellationToken).ConfigureAwait(false));
        return await GetEntityFromBatchAndDbAsync(options, cfd, key, columns, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>TransactionBaseImpl::GetEntityForUpdate: no snapshot without validation.</summary>
    private static void CheckEntityDoValidate(ReadOptions? options, bool doValidate)
    {
        if (!doValidate && options?.Snapshot is not null) ThrowHelper.InvalidArgument("Snapshot must not be set if validation is disabled");
    }

    /// <summary>
    /// Looks up the entities of all keys of <paramref name="batch"/> as this transaction sees them (MultiGetEntity):
    /// keys the transaction's writes do not settle are read from the DB with one MultiGetEntity (one consistent view).
    /// Read the results with <see cref="MultiGetBatch.TryGetEntity"/>, <see cref="MultiGetBatch.GetStatus"/> and
    /// <see cref="MultiGetBatch.GetError"/>; a key's failure (e.g. a merge error) does not fail the others.
    /// </summary>
    public void MultiGetEntity(ReadOptions? options, ColumnFamily columnFamily, MultiGetBatch batch)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(batch);
        ColumnFamilyData cfd = Cfd(columnFamily);
        SetSnapshotIfNeeded();
        var reader = new TxnMultiEntityReader(this, options, cfd);
        _batch.MultiGetEntity(cfd.Id, cfd.MergeOperator, cfd.TimestampSize, options, batch, ref reader);
    }

    public void MultiGetEntity(ReadOptions? options, MultiGetBatch batch) => MultiGetEntity(options, null!, batch);

    private bool GetEntityFromBatchAndDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, WideColumns columns)
    {
        if (WriteBatchWithIndex.CheckEntityReadTimestamp(cfd.TimestampSize, options) is { } e) throw e;
        var reader = new TxnEntityReader(this, options, cfd);
        return _batch.GetEntity(cfd.Id, cfd.MergeOperator, key, columns, ref reader);
    }

    private ValueTask<bool> GetEntityFromBatchAndDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, WideColumns columns,
        CancellationToken cancellationToken)
    {
        if (WriteBatchWithIndex.CheckEntityReadTimestamp(cfd.TimestampSize, options) is { } e) throw e;
        int idx = _batch.Find(cfd.Id, key.Span);
        if (_batch.Classify(idx) == BatchLookupResult.NotFound) return GetEntityFromDbAsync(options, cfd, key, columns, cancellationToken);
        var reader = new TxnEntityReader(this, options, cfd);
        return new ValueTask<bool>(_batch.GetEntity(cfd.Id, cfd.MergeOperator, key.Span, columns, ref reader));
    }

    /// <summary>The DB part of GetEntityFromBatchAndDB (WriteUnprepared reads with its read callback).</summary>
    private protected virtual bool GetEntityFromDb(ReadOptions? options, ColumnFamilyData cfd, ReadOnlySpan<byte> key, WideColumns columns) =>
        ColumnFamilyHandle(cfd).TryGetEntity(options, key, columns);

    /// <summary>Async <see cref="GetEntityFromDb"/>.</summary>
    private protected virtual ValueTask<bool> GetEntityFromDbAsync(ReadOptions? options, ColumnFamilyData cfd, ReadOnlyMemory<byte> key, WideColumns columns,
        CancellationToken cancellationToken) =>
        ColumnFamilyHandle(cfd).TryGetEntityAsync(options, key, columns, cancellationToken);

    /// <summary>The DB part of MultiGetEntityFromBatchAndDB: one MultiGetEntity of <paramref name="keys"/>.</summary>
    private protected virtual void MultiGetEntityFromDb(ReadOptions? options, ColumnFamilyData cfd, MultiGetBatch keys) =>
        ColumnFamilyHandle(cfd).MultiGetEntity(options, keys);

    private readonly struct TxnEntityReader(Transaction txn, ReadOptions? options, ColumnFamilyData cfd) : WriteBatchWithIndex.IEntityDbReader
    {
        public bool TryGetEntity(ReadOnlySpan<byte> key, WideColumns columns) => txn.GetEntityFromDb(options, cfd, key, columns);
    }

    private readonly struct TxnMultiEntityReader(Transaction txn, ReadOptions? options, ColumnFamilyData cfd) : WriteBatchWithIndex.IMultiEntityDbReader
    {
        public void MultiGetEntity(MultiGetBatch keys) => txn.MultiGetEntityFromDb(options, cfd, keys);
    }

    // ---- cross-column-family iterators ----------------------------------------------------------------------

    /// <summary>
    /// A coalescing iterator over this transaction's view of <paramref name="columnFamilies"/>
    /// (Transaction::GetCoalescingIterator): like <see cref="RocksDatabase.NewCoalescingIterator"/>, with each column
    /// family's DB iterator merged with the transaction's writes (<see cref="NewIterator(ColumnFamily, ReadOptions?)"/>).
    /// The DB side reads one consistent view (the snapshot of <paramref name="options"/>, or an implicit one held by the
    /// iterator). InvalidArgument without column families or with different comparators; NotSupported for WritePrepared
    /// and WriteUnprepared transactions, like upstream.
    /// </summary>
    public TransactionCoalescingIterator GetCoalescingIterator(ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        ThrowIfDisposed();
        CheckMultiCfIterator(columnFamilies);
        SetSnapshotIfNeeded();
        var it = new TransactionCoalescingIterator(columnFamilies);
        InitMultiCfIterator(it, options);
        return it;
    }

    /// <summary>An attribute group iterator over this transaction's view (Transaction::GetAttributeGroupIterator); see <see cref="GetCoalescingIterator"/>.</summary>
    public TransactionAttributeGroupIterator GetAttributeGroupIterator(ReadOptions? options, ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        ThrowIfDisposed();
        CheckMultiCfIterator(columnFamilies);
        SetSnapshotIfNeeded();
        var it = new TransactionAttributeGroupIterator(columnFamilies);
        InitMultiCfIterator(it, options);
        return it;
    }

    /// <summary>False for WritePrepared / WriteUnprepared (GetCoalescingIterator / GetAttributeGroupIterator NotSupported).</summary>
    private protected virtual bool SupportsMultiCfIterators => true;

    private void CheckMultiCfIterator(ReadOnlySpan<ColumnFamily> columnFamilies)
    {
        if (!SupportsMultiCfIterators)
        {
            ThrowHelper.NotSupported("GetCoalescingIterator / GetAttributeGroupIterator not supported for write-prepared/write-unprepared transactions");
        }
        // TransactionBaseImpl::NewMultiCfIterator argument checks.
        if (columnFamilies.IsEmpty) ThrowHelper.InvalidArgument("No Column Family was provided");
        Comparator first = columnFamilies[0].Data.UserComparator;
        for (int i = 1; i < columnFamilies.Length; i++)
        {
            Comparator c = columnFamilies[i].Data.UserComparator;
            if (!ReferenceEquals(first, c) && first.Name != c.Name) ThrowHelper.InvalidArgument("Different comparators are being used across CFs");
        }
    }

    /// <summary>db_->NewIterators (one consistent view) + NewIteratorWithBase per column family.</summary>
    private void InitMultiCfIterator(TransactionMultiCfIterator it, ReadOptions? options)
    {
        ReadOnlySpan<ColumnFamily> cfs = it.ColumnFamilies;
        Snapshot? owned = null;
        ReadOptions? ro = options;
        if (cfs.Length > 1 && options?.Snapshot is null)
        {
            owned = _database.GetSnapshot();
            ro = options?.Clone() ?? new ReadOptions();
            ro.Snapshot = owned;
        }
        var children = new TransactionIterator[cfs.Length];
        try
        {
            for (int i = 0; i < cfs.Length; i++)
            {
                if (cfs[i].Db != _database.Impl) ThrowHelper.InvalidArgument("column family does not belong to this DB");
                ColumnFamilyData cfd = cfs[i].Data;
                children[i] = new TransactionIterator(NewDbIterator(cfs[i], cfd, ro), _batch, cfd, ro);
            }
        }
        catch
        {
            foreach (TransactionIterator? c in children) c?.Dispose();
            owned?.Dispose();
            throw;
        }
        it.Init(children, owned);
    }
}
