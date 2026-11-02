using System.Buffers;
using RocksDb.Managed.Db;
using RocksDb.Managed.Utilities.Wbwi;
using RocksDb.Managed.Util;

namespace RocksDb.Managed;

/// <summary>Kind of a <see cref="WriteBatchWithIndex"/> entry (rocksdb::WriteType); values match upstream.</summary>
public enum WriteType : byte
{
    Put = 0,
    Merge = 1,
    Delete = 2,
    SingleDelete = 3,
    DeleteRange = 4,
    LogData = 5,
    Xid = 6,
    PutEntity = 7,
    Unknown = 8,
}

/// <summary>
/// A <see cref="WriteBatch"/> with a searchable index of its keys, so that the pending writes can be read back
/// (<see cref="TryGetFromBatch(ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>), combined with the DB
/// (<see cref="TryGetFromBatchAndDB(RocksDatabase, ReadOptions?, ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>,
/// <see cref="NewIteratorWithBase(ColumnFamily?, Iterator, ReadOptions?)"/>) before being written atomically with
/// <see cref="RocksDatabase.Write(WriteBatchWithIndex)"/>. Port of rocksdb::WriteBatchWithIndex
/// (utilities/write_batch_with_index/write_batch_with_index.cc).
/// <para>
/// The batch rep is byte-identical to the one native WriteBatchWithIndex builds for the same operations. With
/// <c>overwriteKey</c> the index keeps one entry per key for Put/Delete/SingleDelete/PutEntity (merges always add an
/// entry), like upstream's overwrite_key mode; otherwise every update is indexed. Merges are resolved with the column
/// family's merge operator (operations on the default column family without a <see cref="ColumnFamily"/> use the
/// merge operator passed to the reading method). DeleteRange is not supported (NotSupported, like upstream). Not
/// thread-safe. Dispose to return the batch buffer.
/// </para>
/// </summary>
public sealed class WriteBatchWithIndex : IDisposable
{
    private readonly WriteBatch _batch;
    private readonly WriteBatchEntryIndex _index;
    private readonly Comparator _defaultComparator;
    private readonly MergeContext _mergeContext = new();

    /// <summary>A bytewise-ordered batch without overwrite_key.</summary>
    public WriteBatchWithIndex() : this(Comparator.Bytewise, 0, false, 0) { }

    /// <summary>
    /// <paramref name="defaultComparator"/> orders keys of column families for which no <see cref="ColumnFamily"/> was
    /// given (upstream: backup_index_comparator); <paramref name="overwriteKey"/> selects overwrite_key mode;
    /// <paramref name="maxBytes"/> &gt; 0 caps the batch size (MemoryLimit).
    /// </summary>
    public WriteBatchWithIndex(Comparator defaultComparator, int reservedBytes, bool overwriteKey, int maxBytes)
    {
        ArgumentNullException.ThrowIfNull(defaultComparator);
        _defaultComparator = defaultComparator;
        _batch = new WriteBatch(reservedBytes, maxBytes);
        _index = new WriteBatchEntryIndex(_batch, defaultComparator);
        OverwriteKey = overwriteKey;
    }

    /// <summary>
    /// The batch of a transaction (TransactionBaseImpl): overwrite_key mode, and with <paramref name="withNoop"/> (2PC) the
    /// batch starts with a Noop record that Prepare turns into the begin-prepare marker (also after <see cref="Clear"/>).
    /// </summary>
    internal WriteBatchWithIndex(int maxBytes, bool withNoop) : this(Comparator.Bytewise, 0, true, maxBytes)
    {
        _withNoop = withNoop;
        if (withNoop) _batch.InsertNoop();
    }

    private readonly bool _withNoop;

    /// <summary>overwrite_key mode.</summary>
    public bool OverwriteKey { get; }

    /// <summary>The underlying batch (WriteBatchWithIndex::GetWriteBatch). Do not modify it directly.</summary>
    public WriteBatch WriteBatch => _batch;

    /// <summary>The batch rep (WriteBatch::Data()).</summary>
    public ReadOnlySpan<byte> Data => _batch.Data;

    /// <summary>Number of operations in the batch.</summary>
    public int Count => _batch.Count;

    /// <summary>Bytes in the batch rep (GetDataSize).</summary>
    public int DataSize => _batch.DataSize;

    // =====================================================================================================
    // Writes
    // =====================================================================================================

    private uint CfId(ColumnFamily? cf)
    {
        if (cf is null) return 0;
        _index.SetComparator(cf.Id, cf.Data.UserComparator);
        return cf.Id;
    }

    public WriteBatchWithIndex Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Put(null, key, value);

    public WriteBatchWithIndex Put(ColumnFamily? columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Put(CfId(columnFamily), key, value);
        return this;
    }

    public WriteBatchWithIndex PutEntity(ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns) => PutEntity(null, key, columns);

    public WriteBatchWithIndex PutEntity(ColumnFamily? columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<WideColumn> columns)
    {
        uint cf = CfId(columnFamily);
        if (TimestampSizeOf(cf) != 0) ThrowHelper.NotSupported("PutEntity on a column family with user-defined timestamps in WriteBatchWithIndex");
        int offset = _batch.DataSize;
        _batch.PutEntity(cf, key, columns);
        AddOrUpdateIndex(cf, WriteType.PutEntity, offset);
        return this;
    }

    public WriteBatchWithIndex Delete(ReadOnlySpan<byte> key) => Delete(null, key);

    public WriteBatchWithIndex Delete(ColumnFamily? columnFamily, ReadOnlySpan<byte> key)
    {
        Delete(CfId(columnFamily), key);
        return this;
    }

    public WriteBatchWithIndex SingleDelete(ReadOnlySpan<byte> key) => SingleDelete(null, key);

    public WriteBatchWithIndex SingleDelete(ColumnFamily? columnFamily, ReadOnlySpan<byte> key)
    {
        SingleDelete(CfId(columnFamily), key);
        return this;
    }

    public WriteBatchWithIndex Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Merge(null, key, value);

    public WriteBatchWithIndex Merge(ColumnFamily? columnFamily, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Merge(CfId(columnFamily), key, value);
        return this;
    }

    /// <summary>Appends a blob that is written to the WAL but not applied (not indexed).</summary>
    public WriteBatchWithIndex PutLogData(ReadOnlySpan<byte> blob)
    {
        _batch.PutLogData(blob);
        return this;
    }

    /// <summary>Not supported by WriteBatchWithIndex (like upstream): throws NotSupported.</summary>
    public WriteBatchWithIndex DeleteRange(ColumnFamily? columnFamily, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey)
    {
        ThrowHelper.NotSupported("DeleteRange unsupported in WriteBatchWithIndex");
        return this;
    }

    /// <summary>Removes every update (WriteBatchWithIndex::Clear).</summary>
    public void Clear()
    {
        _batch.Clear();
        if (_withNoop) _batch.InsertNoop();
        _index.Clear();
        _hasKeyWithTimestamp = false;
    }

    /// <summary>Records the current state; see <see cref="RollbackToSavePoint"/>.</summary>
    public void SetSavePoint() => _batch.SetSavePoint();

    /// <summary>
    /// Removes the updates added since the most recent save point and rebuilds the index (ReBuildIndex). Returns false
    /// when there is no save point (upstream: NotFound).
    /// </summary>
    public bool RollbackToSavePoint()
    {
        if (!_batch.RollbackToSavePoint()) return false;
        RebuildIndex();
        return true;
    }

    /// <summary>Removes the most recent save point. Returns false when there is none.</summary>
    public bool PopSavePoint() => _batch.PopSavePoint();

    /// <summary>User-defined timestamp size of column family <paramref name="cf"/> (its index comparator's).</summary>
    private int TimestampSizeOf(uint cf) => _index.GetComparator(cf).TimestampSize;

    /// <summary>
    /// WriteBatch::Put(column_family, key, value) on a column family with timestamps: the key gets a dummy (all-zero)
    /// timestamp that the commit replaces (needs_in_place_update_ts_ / UpdateTimestamps).
    /// </summary>
    private ReadOnlySpan<byte> PlaceholderTimestamp(uint cf)
    {
        _hasKeyWithTimestamp = true;
        return s_zeroTimestamp.AsSpan(0, TimestampSizeOf(cf));
    }

    private static readonly byte[] s_zeroTimestamp = new byte[64];
    private bool _hasKeyWithTimestamp;

    /// <summary>
    /// True when a key of a column family with user-defined timestamps was added with a placeholder timestamp
    /// (WriteBatchInternal::HasKeyWithTimestamp): the batch must get its timestamps (commit timestamp) before it is written.
    /// </summary>
    internal bool HasKeyWithTimestamp => _hasKeyWithTimestamp;

    /// <summary>Rep::AddOrUpdateIndexWithCfId (+ UpdateExistingEntryWithCfId in overwrite_key mode).</summary>
    private void AddOrUpdateIndex(uint cf, WriteType type, int recordOffset)
    {
        DecodeKeyPosition(_batch.Data, recordOffset, out int keyOffset, out int keySize);
        // MakeEntry: the index holds the user key without its timestamp.
        int tsSize = TimestampSizeOf(cf);
        if (tsSize > 0 && keySize >= tsSize) keySize -= tsSize;
        if (OverwriteKey && type != WriteType.Merge)
        {
            ReadOnlySpan<byte> key = _batch.Data.Slice(keyOffset, keySize);
            // Seek() finds the first entry with key == `key`: the most recent update.
            int node = _index.Seek(cf, key, long.MaxValue);
            if (node >= 0 && _index.ColumnFamily(node) == cf &&
                _index.GetComparator(cf).CompareWithoutTimestamp(_index.Key(node), false, key, false) == 0)
            {
                _index.UpdateOffset(node, recordOffset);
                return;
            }
        }
        _index.Insert(cf, recordOffset, keyOffset, keySize);
    }

    /// <summary>ReadKeyFromWriteBatchEntry: the position of the key of the record at <paramref name="offset"/>.</summary>
    private static void DecodeKeyPosition(ReadOnlySpan<byte> rep, int offset, out int keyOffset, out int keySize)
    {
        int pos = offset;
        var tag = (EntryType)rep[pos++];
        if (tag is EntryType.ColumnFamilyValue or EntryType.ColumnFamilyDeletion or EntryType.ColumnFamilySingleDeletion or EntryType.ColumnFamilyMerge
            or EntryType.ColumnFamilyRangeDeletion or EntryType.ColumnFamilyWideColumnEntity or EntryType.ColumnFamilyBlobIndex
            or EntryType.ColumnFamilyValuePreferredSeqno)
        {
            pos += Coding.DecodeVarint32(rep[pos..], out _);
        }
        pos += Coding.DecodeVarint32(rep[pos..], out uint len);
        keyOffset = pos;
        keySize = (int)len;
    }

    /// <summary>Rep::ReBuildIndex: re-reads every record of the batch into a fresh index.</summary>
    private void RebuildIndex()
    {
        _index.Clear();
        ReadOnlySpan<byte> rep = _batch.Data;
        int pos = WriteBatch.HeaderSize;
        while (pos < rep.Length)
        {
            int recordOffset = pos;
            string? error = WriteBatchReader.TryReadRecord(rep, ref pos, out WriteBatchEntry e);
            if (error is not null) ThrowHelper.Corruption(error);
            WriteType t = e.Type switch
            {
                EntryType.Value => WriteType.Put,
                EntryType.Deletion => WriteType.Delete,
                EntryType.SingleDeletion => WriteType.SingleDelete,
                EntryType.Merge => WriteType.Merge,
                EntryType.WideColumnEntity => WriteType.PutEntity,
                EntryType.RangeDeletion => WriteType.DeleteRange,
                _ => WriteType.Unknown,
            };
            if (t is WriteType.Unknown or WriteType.DeleteRange) continue; // LogData, XID markers: not indexed
            AddOrUpdateIndex(e.ColumnFamilyId, t, recordOffset);
        }
    }

    // =====================================================================================================
    // Entries
    // =====================================================================================================

    /// <summary>ReadableWriteBatch::GetEntryFromDataOffset.</summary>
    internal WriteType GetEntry(int recordOffset, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        ReadOnlySpan<byte> rep = _batch.Data;
        int pos = recordOffset;
        string? error = WriteBatchReader.TryReadRecord(rep, ref pos, out WriteBatchEntry e);
        if (error is not null) ThrowHelper.Corruption(error);
        key = e.Key;
        // WBWIIteratorImpl::Entry: the entry key never includes the user-defined timestamp.
        int tsSize = TimestampSizeOf(e.ColumnFamilyId);
        if (tsSize > 0 && key.Length >= tsSize) key = key[..^tsSize];
        value = e.Value;
        return e.Type switch
        {
            EntryType.Value => WriteType.Put,
            EntryType.Deletion => WriteType.Delete,
            EntryType.SingleDeletion => WriteType.SingleDelete,
            EntryType.Merge => WriteType.Merge,
            EntryType.WideColumnEntity => WriteType.PutEntity,
            EntryType.RangeDeletion => WriteType.DeleteRange,
            EntryType.LogData => WriteType.LogData,
            _ => WriteType.Xid,
        };
    }

    internal WriteBatchEntryIndex Index => _index;

    // ---- internal column-family-id API (transactions) ----

    /// <summary>Registers the comparator ordering column family <paramref name="cf"/> (SetComparatorForCF).</summary>
    internal void RegisterColumnFamily(uint cf, Comparator comparator) => _index.SetComparator(cf, comparator);

    internal void Put(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        int offset = _batch.DataSize;
        if (TimestampSizeOf(cf) != 0) _batch.Put(cf, key, PlaceholderTimestamp(cf), value);
        else _batch.Put(cf, key, value);
        AddOrUpdateIndex(cf, WriteType.Put, offset);
    }

    internal void PutEntitySerialized(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
    {
        if (TimestampSizeOf(cf) != 0) ThrowHelper.NotSupported("PutEntity on a column family with user-defined timestamps in WriteBatchWithIndex");
        int offset = _batch.DataSize;
        _batch.PutEntitySerialized(cf, key, entity);
        AddOrUpdateIndex(cf, WriteType.PutEntity, offset);
    }

    internal void Delete(uint cf, ReadOnlySpan<byte> key)
    {
        int offset = _batch.DataSize;
        if (TimestampSizeOf(cf) != 0) _batch.Delete(cf, key, PlaceholderTimestamp(cf));
        else _batch.Delete(cf, key);
        AddOrUpdateIndex(cf, WriteType.Delete, offset);
    }

    internal void SingleDelete(uint cf, ReadOnlySpan<byte> key)
    {
        int offset = _batch.DataSize;
        if (TimestampSizeOf(cf) != 0) _batch.SingleDelete(cf, key, PlaceholderTimestamp(cf));
        else _batch.SingleDelete(cf, key);
        AddOrUpdateIndex(cf, WriteType.SingleDelete, offset);
    }

    internal void Merge(uint cf, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        int offset = _batch.DataSize;
        if (TimestampSizeOf(cf) != 0) _batch.Merge(cf, key, PlaceholderTimestamp(cf), value);
        else _batch.Merge(cf, key, value);
        AddOrUpdateIndex(cf, WriteType.Merge, offset);
    }

    /// <summary>
    /// GetFromBatchAndDB's batch part for column family <paramref name="cf"/>: the lookup result, with
    /// <see cref="LookupResult.Deleted"/> reported as <see cref="LookupResult.Found"/> when merge operands follow the delete.
    /// </summary>
    internal LookupResult Lookup(uint cf, ReadOnlySpan<byte> key)
    {
        LookupResult r = FindLatestUpdate(cf, _index.GetComparator(cf), key, _mergeContext, out _, out _);
        return r == LookupResult.Deleted && _mergeContext.Count > 0 ? LookupResult.Found : r;
    }

    /// <summary>
    /// Computes the value of <paramref name="key"/> (MergeKey): the batch's Put/PutEntity (or, when the batch holds only
    /// merges, <paramref name="dbValue"/> when <paramref name="dbHasValue"/>) with the batch's merge operands applied.
    /// Returns false if the key does not exist.
    /// </summary>
    internal bool ComputeValue(uint cf, ReadOnlySpan<byte> key, MergeOperator? mergeOperator, bool dbHasValue, ReadOnlySpan<byte> dbValue,
        IBufferWriter<byte> result)
    {
        LookupResult r = FindLatestUpdate(cf, _index.GetComparator(cf), key, _mergeContext, out WriteType type, out ReadOnlySpan<byte> v);
        bool hasBase;
        ReadOnlySpan<byte> baseValue;
        switch (r)
        {
            case LookupResult.Found:
                hasBase = true;
                baseValue = type == WriteType.PutEntity ? Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(v) : v;
                break;
            case LookupResult.MergeInProgress:
                hasBase = dbHasValue;
                baseValue = dbValue;
                break;
            case LookupResult.Deleted:
                hasBase = false;
                baseValue = default;
                break;
            default:
                if (!dbHasValue) return false;
                result.Write(dbValue);
                return true;
        }
        if (_mergeContext.Count == 0)
        {
            if (!hasBase) return false;
            result.Write(baseValue);
            return true;
        }
        if (mergeOperator is null) ThrowHelper.InvalidArgument("Merge operator not specified for the column family");
        MergeHelper.TimedFullMerge(mergeOperator!, key, hasBase, baseValue, _mergeContext, result);
        return true;
    }

    /// <summary>NewIteratorWithBase for a column family id (transactions).</summary>
    internal BaseDeltaIterator NewIteratorWithBase(uint cf, Comparator comparator, MergeOperator? mergeOperator, Iterator baseIterator,
        ReadOptions? options)
    {
        _index.SetComparator(cf, comparator);
        var delta = new WriteBatchWithIndexIterator(this, cf, comparator, options?.IterateLowerBound, options?.IterateUpperBound);
        return new BaseDeltaIterator(baseIterator, delta, comparator, mergeOperator);
    }

    internal Comparator ComparatorOf(ColumnFamily? cf) => cf is null ? _defaultComparator : _index.GetComparator(CfId(cf));

    /// <summary>
    /// An iterator over the indexed entries of a column family (WriteBatchWithIndex::NewIterator): every update of each
    /// key, keys in comparator order, the most recent update of a key first.
    /// </summary>
    public WriteBatchWithIndexIterator NewIterator() => NewIterator(null);

    public WriteBatchWithIndexIterator NewIterator(ColumnFamily? columnFamily) => new(this, CfId(columnFamily), ComparatorOf(columnFamily), null, null);

    /// <summary>
    /// An iterator over the DB (<paramref name="baseIterator"/>, which the returned iterator owns) with the batch's
    /// updates applied on top (WriteBatchWithIndex::NewIteratorWithBase / BaseDeltaIterator): puts and entities
    /// override, deletes hide, merges are applied to the base value. <see cref="ReadOptions.IterateLowerBound"/> /
    /// <see cref="ReadOptions.IterateUpperBound"/> of <paramref name="options"/> bound the batch side (the base iterator
    /// should be created with the same options).
    /// </summary>
    public BaseDeltaIterator NewIteratorWithBase(Iterator baseIterator) => NewIteratorWithBase(null, baseIterator, null);

    public BaseDeltaIterator NewIteratorWithBase(ColumnFamily? columnFamily, Iterator baseIterator, ReadOptions? options)
    {
        ArgumentNullException.ThrowIfNull(baseIterator);
        uint cf = CfId(columnFamily);
        Comparator cmp = ComparatorOf(columnFamily);
        var delta = new WriteBatchWithIndexIterator(this, cf, cmp, options?.IterateLowerBound, options?.IterateUpperBound);
        return new BaseDeltaIterator(baseIterator, delta, cmp, columnFamily?.Data.MergeOperator);
    }

    // =====================================================================================================
    // Reads
    // =====================================================================================================

    internal enum LookupResult : byte { NotFound, Found, Deleted, MergeInProgress }

    /// <summary>
    /// GetFromBatchImpl's search: the most recent Put/PutEntity/Delete of <paramref name="key"/> in the batch, with the
    /// merge operands above it collected (newest first) into <paramref name="context"/>.
    /// </summary>
    internal LookupResult FindLatestUpdate(uint cf, Comparator cmp, ReadOnlySpan<byte> key, MergeContext context, out WriteType type,
        out ReadOnlySpan<byte> value)
    {
        context.Clear();
        type = WriteType.Unknown;
        value = default;
        LookupResult result = LookupResult.NotFound;
        for (int node = _index.Seek(cf, key, long.MaxValue); node >= 0; node = _index.NextNode(node))
        {
            if (_index.ColumnFamily(node) != cf || cmp.CompareWithoutTimestamp(_index.Key(node), false, key, false) != 0) break;
            WriteType t = GetEntry(_index.RecordOffset(node), out _, out ReadOnlySpan<byte> v);
            switch (t)
            {
                case WriteType.Put:
                case WriteType.PutEntity:
                    type = t;
                    value = v;
                    return LookupResult.Found;
                case WriteType.Delete:
                case WriteType.SingleDelete:
                    type = t;
                    return LookupResult.Deleted;
                case WriteType.Merge:
                    result = LookupResult.MergeInProgress;
                    context.PushOperand(v);
                    break;
            }
        }
        return result;
    }

    private static MergeOperator RequireMergeOperator(MergeOperator? op)
    {
        if (op is null) ThrowHelper.InvalidArgument("Merge operator must be set for column family");
        return op!;
    }

    /// <summary>
    /// Reads <paramref name="key"/> from the batch only (WriteBatchWithIndex::GetFromBatch) and appends its value to
    /// <paramref name="value"/>. Returns false if the key is not in the batch or deleted there. Throws
    /// <see cref="StatusCode.MergeInProgress"/> when the batch only holds merge operands for the key (the base value is
    /// in the DB). Merge operands on top of a Put/Delete are applied with the column family's merge operator.
    /// </summary>
    public bool TryGetFromBatch(ColumnFamily? columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value) =>
        TryGetFromBatch(columnFamily, columnFamily?.Data.MergeOperator, key, value);

    public bool TryGetFromBatch(ReadOnlySpan<byte> key, IBufferWriter<byte> value) => TryGetFromBatch(null, null, key, value);

    /// <summary><see cref="TryGetFromBatch(ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/> with an explicit merge operator.</summary>
    public bool TryGetFromBatch(ColumnFamily? columnFamily, MergeOperator? mergeOperator, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        ArgumentNullException.ThrowIfNull(value);
        uint cf = CfId(columnFamily);
        LookupResult r = FindLatestUpdate(cf, ComparatorOf(columnFamily), key, _mergeContext, out WriteType type, out ReadOnlySpan<byte> v);
        switch (r)
        {
            case LookupResult.NotFound:
                return false;
            case LookupResult.MergeInProgress:
                throw new RocksDbException(StatusCode.MergeInProgress, "Merge in progress");
            case LookupResult.Deleted:
                if (_mergeContext.Count == 0) return false;
                MergeHelper.TimedFullMerge(RequireMergeOperator(mergeOperator), key, false, default, _mergeContext, value);
                return true;
            default: // Found
                ReadOnlySpan<byte> baseValue = type == WriteType.PutEntity ? Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(v) : v;
                if (_mergeContext.Count == 0) value.Write(baseValue);
                else MergeHelper.TimedFullMerge(RequireMergeOperator(mergeOperator), key, true, baseValue, _mergeContext, value);
                return true;
        }
    }

    /// <summary>Copying convenience form of <see cref="TryGetFromBatch(ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>.</summary>
    public byte[]? GetFromBatch(ColumnFamily? columnFamily, ReadOnlySpan<byte> key)
    {
        var w = new ArrayBufferWriter<byte>();
        return TryGetFromBatch(columnFamily, key, w) ? w.WrittenSpan.ToArray() : null;
    }

    public byte[]? GetFromBatch(ReadOnlySpan<byte> key) => GetFromBatch(null, key);

    /// <summary>
    /// Reads the entity of <paramref name="key"/> from the batch only (GetEntityFromBatch): a Put reads as a single default
    /// column; merges apply to the default column. Same not-found / MergeInProgress rules as
    /// <see cref="TryGetFromBatch(ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>.
    /// </summary>
    public bool TryGetEntityFromBatch(ColumnFamily? columnFamily, ReadOnlySpan<byte> key, WideColumns columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        uint cf = CfId(columnFamily);
        LookupResult r = FindLatestUpdate(cf, ComparatorOf(columnFamily), key, _mergeContext, out WriteType type, out ReadOnlySpan<byte> v);
        PooledBufferWriter w = ObjectPools.RentWriter();
        try
        {
            switch (r)
            {
                case LookupResult.NotFound:
                    columns.Clear();
                    return false;
                case LookupResult.MergeInProgress:
                    columns.Clear();
                    throw new RocksDbException(StatusCode.MergeInProgress, "Merge in progress");
                case LookupResult.Deleted:
                    if (_mergeContext.Count == 0)
                    {
                        columns.Clear();
                        return false;
                    }
                    MergeHelper.TimedFullMerge(RequireMergeOperator(columnFamily?.Data.MergeOperator), key, false, default, _mergeContext, w);
                    WrapDefault(w, columns);
                    return true;
                default:
                    if (_mergeContext.Count == 0)
                    {
                        if (type == WriteType.PutEntity) columns.SetSerialized(v);
                        else
                        {
                            Db.Wide.WideColumnSerialization.SerializeDefaultColumn(v, w);
                            columns.SetSerialized(w.WrittenSpan);
                        }
                        return true;
                    }
                    MergeOperator op = RequireMergeOperator(columnFamily?.Data.MergeOperator);
                    if (type == WriteType.PutEntity)
                    {
                        MergeHelper.TimedFullMerge(op, key, true, Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(v), _mergeContext, w);
                        PooledBufferWriter e = ObjectPools.RentWriter();
                        try
                        {
                            Db.Wide.WideColumnSerialization.ReplaceDefaultColumn(v, w.WrittenSpan, e);
                            columns.SetSerialized(e.WrittenSpan);
                        }
                        finally
                        {
                            ObjectPools.ReturnWriter(e);
                        }
                        return true;
                    }
                    MergeHelper.TimedFullMerge(op, key, true, v, _mergeContext, w);
                    WrapDefault(w, columns);
                    return true;
            }
        }
        finally
        {
            ObjectPools.ReturnWriter(w);
        }
    }

    private static void WrapDefault(PooledBufferWriter value, WideColumns columns)
    {
        PooledBufferWriter e = ObjectPools.RentWriter();
        try
        {
            Db.Wide.WideColumnSerialization.SerializeDefaultColumn(value.WrittenSpan, e);
            columns.SetSerialized(e.WrittenSpan);
        }
        finally
        {
            ObjectPools.ReturnWriter(e);
        }
    }

    /// <summary>
    /// Reads <paramref name="key"/> from the batch, falling back to (and merging with) the DB
    /// (WriteBatchWithIndex::GetFromBatchAndDB): a Put/Delete in the batch wins; merge operands in the batch are applied
    /// on top of the DB value read with <paramref name="options"/>. Returns false if the key does not exist.
    /// </summary>
    public bool TryGetFromBatchAndDB(RocksDatabase db, ReadOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key, IBufferWriter<byte> value)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(value);
        ColumnFamily cfh = columnFamily ?? db.DefaultColumnFamily;
        uint cf = CfId(cfh);
        if (cfh.Data.TimestampSize > 0 && options?.Timestamp is null) ThrowHelper.InvalidArgument("Must specify timestamp");
        LookupResult r = FindLatestUpdate(cf, ComparatorOf(cfh), key, _mergeContext, out WriteType type, out ReadOnlySpan<byte> v);
        MergeOperator? op = cfh.Data.MergeOperator;
        switch (r)
        {
            case LookupResult.Found:
                {
                    ReadOnlySpan<byte> baseValue = type == WriteType.PutEntity ? Db.Wide.WideColumnSerialization.GetValueOfDefaultColumn(v) : v;
                    if (_mergeContext.Count == 0) value.Write(baseValue);
                    else MergeHelper.TimedFullMerge(RequireMergeOperator(op), key, true, baseValue, _mergeContext, value);
                    return true;
                }
            case LookupResult.Deleted:
                if (_mergeContext.Count == 0) return false;
                MergeHelper.TimedFullMerge(RequireMergeOperator(op), key, false, default, _mergeContext, value);
                return true;
            case LookupResult.NotFound:
                return cfh.TryGet(options, key, value);
            default: // MergeInProgress: merge the batch operands on top of the DB value (MergeAcrossBatchAndDB).
                {
                    MergeOperator mop = RequireMergeOperator(op);
                    // The DB read may reuse pooled state: keep the operands while it runs.
                    var existing = new WideColumns();
                    bool found = cfh.TryGetEntity(options, key, existing);
                    MergeHelper.TimedFullMerge(mop, key, found, found ? existing.DefaultColumnValue : default, _mergeContext, value);
                    return true;
                }
        }
    }

    public bool TryGetFromBatchAndDB(RocksDatabase db, ReadOnlySpan<byte> key, IBufferWriter<byte> value) =>
        TryGetFromBatchAndDB(db, null, null, key, value);

    /// <summary>Copying convenience form of <see cref="TryGetFromBatchAndDB(RocksDatabase, ReadOptions?, ColumnFamily?, ReadOnlySpan{byte}, IBufferWriter{byte})"/>.</summary>
    public byte[]? GetFromBatchAndDB(RocksDatabase db, ReadOptions? options, ColumnFamily? columnFamily, ReadOnlySpan<byte> key)
    {
        var w = new ArrayBufferWriter<byte>();
        return TryGetFromBatchAndDB(db, options, columnFamily, key, w) ? w.WrittenSpan.ToArray() : null;
    }

    public byte[]? GetFromBatchAndDB(RocksDatabase db, ReadOnlySpan<byte> key) => GetFromBatchAndDB(db, null, null, key);

    public void Dispose()
    {
        _batch.Dispose();
        _mergeContext.Dispose();
    }
}

public sealed partial class RocksDatabase
{
    /// <summary>Atomically applies the batch of <paramref name="batch"/> (DB::Write(batch->GetWriteBatch())).</summary>
    public void Write(WriteBatchWithIndex batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        Write(null, batch.WriteBatch);
    }

    public void Write(WriteOptions? options, WriteBatchWithIndex batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        Write(options, batch.WriteBatch);
    }

    public ValueTask WriteAsync(WriteOptions? options, WriteBatchWithIndex batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);
        return WriteAsync(options, batch.WriteBatch, cancellationToken);
    }
}
