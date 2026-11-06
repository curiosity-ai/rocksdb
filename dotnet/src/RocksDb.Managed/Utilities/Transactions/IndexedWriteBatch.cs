using System.Buffers;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Utilities.Transactions;

/// <summary>Result of a lookup of one key in an <see cref="IndexedWriteBatch"/> (WBWIIteratorImpl::Result).</summary>
internal enum BatchLookupResult : byte
{
    /// <summary>The batch has no entry for the key.</summary>
    NotFound,
    /// <summary>The batch determines the value (a put, or a delete followed by merges, possibly with merges on top).</summary>
    Found,
    /// <summary>The newest base entry is a deletion and no merges follow it.</summary>
    Deleted,
    /// <summary>Only merges: the base value must come from the DB.</summary>
    MergeInProgress,
}

/// <summary>
/// A transaction's write batch (TransactionBaseImpl::write_batch_): a <see cref="WriteBatchWithIndex"/> in
/// overwrite_key mode (with the 2PC Noop placeholder when allow_2pc), addressed by column family id. Each column family's
/// comparator is resolved from the DB when the family is first written.
/// </summary>
internal sealed class IndexedWriteBatch : IDisposable
{
    private readonly WriteBatchWithIndex _wbwi;
    private readonly Func<uint, Comparator> _comparatorOf;
    private ulong _registered;               // bit per column family id < 64 already registered
    private byte[] _findKey = new byte[64];  // key of the last Find
    private int _findKeyLength;
    private uint _findCf;

    public IndexedWriteBatch(bool withNoop, int maxBytes, Func<uint, Comparator> comparatorOf)
    {
        _wbwi = new WriteBatchWithIndex(maxBytes, withNoop);
        _comparatorOf = comparatorOf;
    }

    /// <summary>The indexed batch.</summary>
    public WriteBatchWithIndex Wbwi => _wbwi;

    /// <summary>The underlying batch (its rep is what is written to the WAL / memtables).</summary>
    public WriteBatch Batch => _wbwi.WriteBatch;

    public int Count => _wbwi.Count;

    /// <summary>WriteBatchWithIndex::SubBatchCnt.</summary>
    public int SubBatchCount => _wbwi.SubBatchCount;

    /// <summary>Bytes in the batch rep (GetDataSize).</summary>
    public int DataSize => _wbwi.DataSize;

    private void Register(uint cf)
    {
        if (cf < 64 && (_registered & (1UL << (int)cf)) != 0) return;
        _wbwi.RegisterColumnFamily(cf, _comparatorOf(cf));
        if (cf < 64) _registered |= 1UL << (int)cf;
    }

    public void Clear() => _wbwi.Clear();

    public void Dispose() => _wbwi.Dispose();

    public void Put(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Register(cf);
        _wbwi.Put(cf, key, value);
    }

    public void PutEntitySerialized(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
    {
        Register(cf);
        _wbwi.PutEntitySerialized(cf, key, entity);
    }

    public void PutEntity(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns)
    {
        Register(cf);
        _wbwi.PutEntity(cf, key, columns);
    }

    /// <summary>GetEntityFromBatchAndDB with the DB read through <paramref name="reader"/>.</summary>
    public bool GetEntity<TReader>(uint cf, MergeOperator? mergeOperator, ReadOnlySpan<byte> key, WideColumns columns, ref TReader reader)
        where TReader : struct, WriteBatchWithIndex.IEntityDbReader
    {
        Register(cf);
        return _wbwi.GetEntityFromBatchAndDBCore(cf, mergeOperator, key, columns, ref reader);
    }

    /// <summary>MultiGetEntityFromBatchAndDB with the DB read through <paramref name="reader"/>.</summary>
    public void MultiGetEntity<TReader>(uint cf, MergeOperator? mergeOperator, int timestampSize, ReadOptions? options, MultiGetBatch batch, ref TReader reader)
        where TReader : struct, WriteBatchWithIndex.IMultiEntityDbReader
    {
        Register(cf);
        _wbwi.MultiGetEntityFromBatchAndDBCore(cf, mergeOperator, timestampSize, options, batch, ref reader);
    }

    /// <summary>MultiGetFromBatchAndDB with the DB read through <paramref name="reader"/>.</summary>
    public void MultiGet<TReader>(uint cf, MergeOperator? mergeOperator, int timestampSize, ReadOptions? options, MultiGetBatch batch, ref TReader reader)
        where TReader : struct, WriteBatchWithIndex.IMultiValueDbReader
    {
        Register(cf);
        _wbwi.MultiGetFromBatchAndDBCore(cf, mergeOperator, timestampSize, options, batch, ref reader);
    }

    /// <summary>indexing_enabled_ (Transaction::DisableIndexing / EnableIndexing).</summary>
    public bool IndexingEnabled
    {
        get => !_wbwi.IndexingDisabled;
        set => _wbwi.IndexingDisabled = !value;
    }

    public void Delete(uint cf, ReadOnlySpan<byte> key)
    {
        Register(cf);
        _wbwi.Delete(cf, key);
    }

    public void SingleDelete(uint cf, ReadOnlySpan<byte> key)
    {
        Register(cf);
        _wbwi.SingleDelete(cf, key);
    }

    public void Merge(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Register(cf);
        _wbwi.Merge(cf, key, value);
    }

    public void PutLogData(ReadOnlySpan<byte> blob) => _wbwi.PutLogData(blob);

    public void SetSavePoint() => _wbwi.SetSavePoint();

    /// <summary>WriteBatchWithIndex::RollbackToSavePoint: rolls the batch back and rebuilds the index.</summary>
    public bool RollbackToSavePoint() => _wbwi.RollbackToSavePoint();

    public bool PopSavePoint() => _wbwi.PopSavePoint();

    /// <summary>Looks up a key; returns a token for <see cref="Classify"/> / <see cref="ComputeValue"/> (-1 if the batch has none).</summary>
    public int Find(uint cf, ReadOnlySpan<byte> key)
    {
        Register(cf);
        if (_findKey.Length < key.Length) _findKey = new byte[Math.Max(key.Length, _findKey.Length * 2)];
        key.CopyTo(_findKey);
        _findKeyLength = key.Length;
        _findCf = cf;
        _lastResult = _wbwi.Lookup(cf, key) switch
        {
            WriteBatchWithIndex.LookupResult.Found => BatchLookupResult.Found,
            WriteBatchWithIndex.LookupResult.Deleted => BatchLookupResult.Deleted,
            WriteBatchWithIndex.LookupResult.MergeInProgress => BatchLookupResult.MergeInProgress,
            _ => BatchLookupResult.NotFound,
        };
        return _lastResult == BatchLookupResult.NotFound ? -1 : 0;
    }

    private BatchLookupResult _lastResult;

    /// <summary>Classifies the result of the last <see cref="Find"/> (see <see cref="BatchLookupResult"/>).</summary>
    public BatchLookupResult Classify(int idx) => idx < 0 ? BatchLookupResult.NotFound : _lastResult;

    /// <summary>
    /// Computes the value of the key of the last <see cref="Find"/>: the batch's base value (or, when the batch has only
    /// merges, <paramref name="dbValue"/> / no value) with the batch's merges applied. Returns false if the key does not exist.
    /// </summary>
    public bool ComputeValue(int idx, MergeOperator? mergeOperator, bool dbHasValue, ReadOnlySpan<byte> dbValue, IBufferWriter<byte> result) =>
        _wbwi.ComputeValue(_findCf, _findKey.AsSpan(0, _findKeyLength), mergeOperator, dbHasValue, dbValue, result);
}
