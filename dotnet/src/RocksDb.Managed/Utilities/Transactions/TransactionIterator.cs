using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>
/// Iterator over a transaction's view of a column family: the DB iterator merged with the transaction's writes
/// (TransactionBaseImpl::GetIterator = WriteBatchWithIndex::NewIteratorWithBase). A thin wrapper over
/// <see cref="BaseDeltaIterator"/>: on equal keys the transaction's entry wins; deleted keys are skipped; merges in the
/// transaction are applied on top of the DB value. The iterate bounds of the read options apply to both sides. Writes
/// made through the transaction while the iterator is open are seen by later positioning calls. Not thread-safe.
/// </summary>
public sealed class TransactionIterator : IDisposable
{
    private readonly BaseDeltaIterator _it;

    internal TransactionIterator(Iterator baseIterator, IndexedWriteBatch batch, ColumnFamilyData cfd, ReadOptions? options)
    {
        _it = batch.Wbwi.NewIteratorWithBase(cfd.Id, cfd.UserComparator, cfd.MergeOperator, baseIterator, options);
    }

    internal TransactionIterator(Iterator baseIterator, IndexedWriteBatch batch, ColumnFamilyData cfd) : this(baseIterator, batch, cfd, null) { }

    public bool Valid => _it.Valid;

    public ReadOnlySpan<byte> Key
    {
        get
        {
            if (!_it.Valid) ThrowHelper.InvalidArgument("iterator is not valid");
            return _it.Key;
        }
    }

    /// <summary>The user-defined timestamp of the current DB entry (empty for this transaction's own writes).</summary>
    public ReadOnlySpan<byte> Timestamp
    {
        get
        {
            if (!_it.Valid) ThrowHelper.InvalidArgument("iterator is not valid");
            return _it.Timestamp;
        }
    }

    public ReadOnlySpan<byte> Value
    {
        get
        {
            if (!_it.Valid) ThrowHelper.InvalidArgument("iterator is not valid");
            return _it.Value;
        }
    }

    /// <summary>The wide columns of the current entry (a plain value is a single default column).</summary>
    public WideColumns Columns => _it.Columns;

    public void SeekToFirst() => _it.SeekToFirst();
    public void SeekToLast() => _it.SeekToLast();
    public void Seek(ReadOnlySpan<byte> target) => _it.Seek(target);
    public void SeekForPrev(ReadOnlySpan<byte> target) => _it.SeekForPrev(target);

    public void Next()
    {
        if (!_it.Valid) ThrowHelper.InvalidArgument("iterator is not valid");
        _it.Next();
    }

    public void Prev()
    {
        if (!_it.Valid) ThrowHelper.InvalidArgument("iterator is not valid");
        _it.Prev();
    }

    public void Dispose() => _it.Dispose();
}
