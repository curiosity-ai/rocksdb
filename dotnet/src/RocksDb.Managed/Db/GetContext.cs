using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// State of one point lookup as it walks memtables and table files newest to oldest. Port of rocksdb::GetContext
/// (table/get_context.cc: SaveValue) and the final merge step of DBImpl::GetImpl / MemTable::Get
/// (db/db_impl/db_impl.cc).
/// <para>
/// Values found in table files are copied into <see cref="Value"/> (the visitor's spans are only valid during the
/// callback). Merge operands are collected newest first into a <see cref="MergeContext"/> and combined with
/// <see cref="Managed.MergeOperator.FullMerge"/> at the end.
/// The final merge uses <see cref="MergeHelper.TimedFullMerge(MergeOperator, ReadOnlySpan{byte}, bool, ReadOnlySpan{byte}, MergeContext, IBufferWriter{byte})"/>.
/// </para>
/// Instances are pooled (<see cref="Rent"/>/<see cref="Return"/>) and not thread-safe.
/// </summary>
internal sealed class GetContext
{
    public enum GetState : byte
    {
        NotFound,
        Found,
        Deleted,
        Corrupt,
        Merge,
    }

    public readonly MergeContext MergeContext = new();
    public readonly PooledBufferWriter Value = new(256);
    /// <summary>Scratch output for async table lookups.</summary>
    public readonly PooledBufferWriter Scratch = new(256);

    public GetState State;
    /// <summary>The lookup finished in a memtable (MEMTABLE_HIT statistics).</summary>
    public bool FromMemTable;
    /// <summary>The found value (<see cref="Value"/>) is a serialized wide-column entity.</summary>
    public bool ValueIsEntity;
    /// <summary>
    /// The found value (<see cref="Value"/>) is a blob index: the read path replaces it with the blob
    /// (<see cref="ResolveBlob"/>) while it still holds the version the index was read from.
    /// </summary>
    public bool ValueIsBlobIndex;
    /// <summary>
    /// The found value is a version 2 wide-column entity with blob-backed columns: <see cref="ResolveBlob"/> replaces it
    /// with the resolved (version 1) entity.
    /// </summary>
    public bool ValueEntityHasBlobColumns;

    /// <summary>The found value still references blob files (<see cref="ValueIsBlobIndex"/> or <see cref="ValueEntityHasBlobColumns"/>).</summary>
    public bool NeedsBlobResolution => ValueIsBlobIndex || ValueEntityHasBlobColumns;
    public ulong MaxCoveringTombstoneSeq;
    /// <summary>
    /// Sequence number of the read. With user-defined timestamps table entries with a larger seqno are skipped (they
    /// sort after the lookup key when their timestamp is older).
    /// </summary>
    public ulong ReadSequence = DbFormat.MaxSequenceNumber;
    public MergeOperator? MergeOperator;
    /// <summary>
    /// User-defined timestamp size of the column family being read (0 = timestamps disabled). When non-zero, the
    /// timestamp of the first (newest visible) entry matched is captured (GetContext::SaveValue, timestamp_).
    /// </summary>
    public int TimestampSize;
    /// <summary>User key with timestamp of the blob index found (timestamped lookups only).</summary>
    private readonly PooledBufferWriter _blobUserKey = new(32);
    /// <summary>True once <see cref="FoundTimestamp"/> holds the timestamp of the first matched entry.</summary>
    public bool HasTimestamp;
    private byte[] _timestamp = new byte[8];

    /// <summary>Timestamp of the newest visible entry of the key. REQUIRES: <see cref="HasTimestamp"/>.</summary>
    public ReadOnlySpan<byte> FoundTimestamp => _timestamp.AsSpan(0, TimestampSize);

    /// <summary>The timestamp in <see cref="FoundTimestamp"/> is the one of a covering range tombstone (ts_from_rangetombstone_).</summary>
    private bool _tsFromRangeTombstone;

    /// <summary>
    /// Records the timestamp of the first matched entry (GetContext::SaveValue, timestamp_): later entries are ignored,
    /// except that the first entry newer than the covering range tombstone replaces a timestamp taken from it.
    /// </summary>
    public void RecordTimestampOfUserKey(ReadOnlySpan<byte> userKeyWithTimestamp, ulong sequence)
    {
        int n = TimestampSize;
        if (HasTimestamp)
        {
            if (!_tsFromRangeTombstone || sequence <= MaxCoveringTombstoneSeq) return;
            _tsFromRangeTombstone = false;
        }
        if (_timestamp.Length < n) _timestamp = new byte[n];
        userKeyWithTimestamp[^n..].CopyTo(_timestamp);
        HasTimestamp = true;
    }

    /// <summary>
    /// A range tombstone newer than anything seen so far covers the key: its timestamp is reported unless a point entry
    /// with a higher seqno is found (GetContext::SetTimestampFromRangeTombstone).
    /// </summary>
    public void SetTimestampFromRangeTombstone(ReadOnlySpan<byte> timestamp)
    {
        int n = TimestampSize;
        if (n == 0 || timestamp.Length != n) return;
        if (_timestamp.Length < n) _timestamp = new byte[n];
        timestamp.CopyTo(_timestamp);
        HasTimestamp = true;
        _tsFromRangeTombstone = true;
    }

    public void Reset(MergeOperator? mergeOperator)
    {
        MergeContext.Clear();
        Value.Clear();
        State = GetState.NotFound;
        FromMemTable = false;
        ValueIsEntity = false;
        ValueIsBlobIndex = false;
        ValueEntityHasBlobColumns = false;
        MaxCoveringTombstoneSeq = 0;
        ReadSequence = DbFormat.MaxSequenceNumber;
        MergeOperator = mergeOperator;
        TimestampSize = 0;
        HasTimestamp = false;
        _tsFromRangeTombstone = false;
        _blobUserKey.Clear();
    }

    /// <summary>
    /// Port of GetContext::SaveValue for one entry of a table file (user key already matched by the reader, sequence
    /// visible to the lookup). Returns true to keep scanning older entries of the same key.
    /// </summary>
    public bool SaveValue(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out ulong seq, out EntryType type);
        if (TimestampSize != 0)
        {
            if (seq > ReadSequence) return true; // not in the read snapshot: continue to the next entry
            RecordTimestampOfUserKey(DbFormat.ExtractUserKey(internalKey), seq);
            // ukey_with_ts_found_: blob records are keyed (and verified) by the user key with its timestamp.
            if (type is EntryType.BlobIndex or EntryType.WideColumnEntity && MaxCoveringTombstoneSeq <= seq)
            {
                _blobUserKey.Clear();
                _blobUserKey.Write(DbFormat.ExtractUserKey(internalKey));
            }
        }
        return SaveEntry(seq, type, value);
    }

    /// <summary><see cref="SaveValue"/> with the key footer already decoded.</summary>
    public bool SaveEntry(ulong seq, EntryType type, ReadOnlySpan<byte> value)
    {
        if (MaxCoveringTombstoneSeq > seq && type is EntryType.Value or EntryType.Merge or EntryType.Deletion or EntryType.SingleDeletion
                or EntryType.DeletionWithTimestamp or EntryType.ValuePreferredSeqno or EntryType.BlobIndex or EntryType.WideColumnEntity)
        {
            type = EntryType.RangeDeletion;
        }
        switch (type)
        {
            case EntryType.Value:
            case EntryType.WideColumnEntity:
            case EntryType.BlobIndex:
                // A blob index is kept as the found value and resolved (also as a merge base value) by the read path.
                Value.Clear();
                Value.Write(value);
                ValueIsEntity = type == EntryType.WideColumnEntity;
                ValueIsBlobIndex = type == EntryType.BlobIndex;
                ValueEntityHasBlobColumns = ValueIsEntity && Wide.WideColumnSerialization.HasBlobColumns(value);
                State = GetState.Found;
                return false;
            case EntryType.ValuePreferredSeqno:
                Value.Clear();
                Value.Write(value[..^sizeof(ulong)]);
                State = GetState.Found;
                return false;
            case EntryType.Deletion:
            case EntryType.DeletionWithTimestamp:
            case EntryType.SingleDeletion:
            case EntryType.RangeDeletion:
                State = GetState.Deleted;
                return false;
            case EntryType.Merge:
                State = GetState.Merge;
                MergeContext.PushOperand(value);
                return true;
            default:
                State = GetState.Corrupt;
                return false;
        }
    }

    /// <summary>
    /// Applies a memtable lookup result. Returns true if the lookup is finished (a value or deletion was found).
    /// </summary>
    public bool ApplyMemTableResult(MemTableGetResult r, ReadOnlySpan<byte> value, EntryType valueType)
    {
        switch (r)
        {
            case MemTableGetResult.Found:
                Value.Clear();
                Value.Write(value);
                ValueIsEntity = valueType == EntryType.WideColumnEntity;
                ValueIsBlobIndex = valueType == EntryType.BlobIndex;
                ValueEntityHasBlobColumns = ValueIsEntity && Wide.WideColumnSerialization.HasBlobColumns(value);
                State = GetState.Found;
                return true;
            case MemTableGetResult.Deleted:
                State = GetState.Deleted;
                return true;
            case MemTableGetResult.MergeInProgress:
                State = GetState.Merge;
                return false;
            case MemTableGetResult.Corruption:
                State = GetState.Corrupt;
                return true;
            default:
                return false;
        }
    }

    public bool Done => State is GetState.Found or GetState.Deleted or GetState.Corrupt;

    /// <summary>
    /// GetContext::GetBlobValue: replaces a found blob index with the blob it references, read through
    /// <paramref name="source"/> from <paramref name="version"/> (the version the lookup read). No-op unless
    /// <see cref="ValueIsBlobIndex"/>.
    /// </summary>
    public void ResolveBlob(Blob.BlobSource source, VersionStorageInfo version, ReadOptions? options, ReadOnlySpan<byte> userKey)
    {
        if (ValueEntityHasBlobColumns)
        {
            ResolveEntityBlobColumns(source, version, options, userKey);
            return;
        }
        if (!ValueIsBlobIndex) return;
        if (TimestampSize != 0 && _blobUserKey.WrittenCount > 0) userKey = _blobUserKey.WrittenSpan;
        Scratch.Clear();
        source.GetBlob(version, options, userKey, Value.WrittenSpan, Scratch);
        Value.Clear();
        Value.Write(Scratch.WrittenSpan);
        ValueIsBlobIndex = false;
    }

    /// <summary>Async <see cref="ResolveBlob"/>; <paramref name="userKey"/> must stay valid until completion.</summary>
    public ValueTask ResolveBlobAsync(Blob.BlobSource source, VersionStorageInfo version, ReadOptions? options, ReadOnlyMemory<byte> userKey,
        CancellationToken cancellationToken)
    {
        if (ValueEntityHasBlobColumns)
        {
            // Entity blob columns are resolved with synchronous reads (cache hits are the common case for hot entities).
            ResolveEntityBlobColumns(source, version, options, userKey.Span);
            return ValueTask.CompletedTask;
        }
        if (!ValueIsBlobIndex) return ValueTask.CompletedTask;
        // With user-defined timestamps the blob record is keyed by the user key with the version's timestamp.
        if (TimestampSize != 0 && _blobUserKey.WrittenCount > 0) userKey = _blobUserKey.WrittenMemory;
        Scratch.Clear();
        ValueTask t = source.GetBlobAsync(version, options, userKey, Value.WrittenSpan, Scratch, cancellationToken);
        if (t.IsCompletedSuccessfully)
        {
            t.GetAwaiter().GetResult();
            FinishBlob();
            return ValueTask.CompletedTask;
        }
        return AwaitBlobAsync(t);
    }

    private List<Wide.WideColumnSlot>? _slots;
    private PooledBufferWriter? _entityScratch;

    /// <summary>
    /// GetContext::SaveValue for a version 2 entity (WideColumnSerialization::ResolveEntityBlobColumns): replaces the
    /// found entity with its fully resolved version 1 form.
    /// </summary>
    private void ResolveEntityBlobColumns(Blob.BlobSource source, VersionStorageInfo version, ReadOptions? options, ReadOnlySpan<byte> userKey)
    {
        if (TimestampSize != 0 && _blobUserKey.WrittenCount > 0) userKey = _blobUserKey.WrittenSpan;
        var fetcher = new Blob.VersionBlobFetcher(source, version, options);
        Scratch.Clear();
        _entityScratch ??= new PooledBufferWriter(256);
        Wide.WideColumnSerialization.ResolveEntityBlobColumns(Value.WrittenSpan, userKey, ref fetcher, _slots ??= new List<Wide.WideColumnSlot>(8),
            _entityScratch, Scratch);
        Value.Clear();
        Value.Write(Scratch.WrittenSpan);
        ValueEntityHasBlobColumns = false;
    }

    private async ValueTask AwaitBlobAsync(ValueTask pending)
    {
        await pending.ConfigureAwait(false);
        FinishBlob();
    }

    private void FinishBlob()
    {
        Value.Clear();
        Value.Write(Scratch.WrittenSpan);
        ValueIsBlobIndex = false;
    }

    /// <summary>
    /// Final step: throws for corruption / unsupported entries, applies merge operands, and returns the final value
    /// (or false if the key does not exist). The span is valid until the context is reused.
    /// </summary>
    public bool TryResolve(ReadOnlySpan<byte> userKey, out ReadOnlySpan<byte> result)
    {
        result = default;
        switch (State)
        {
            case GetState.Corrupt:
                ThrowHelper.Corruption("corrupted entry type for key");
                return false;
        }
        if (NeedsBlobResolution) ThrowUnresolvedBlob();
        if (MergeContext.Count == 0)
        {
            if (State != GetState.Found) return false;
            // A wide-column entity reads as the value of its default column (GetContext::SaveValue, kTypeWideColumnEntity).
            result = ValueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(Value.WrittenSpan) : Value.WrittenSpan;
            return true;
        }
        // Merge operands present: combine with the base value (if found) or with no base (deleted / nothing older).
        if (MergeOperator is null) throw RocksDbException.InvalidArgument("merge_operator is not properly initialized.");
        bool hasBase = State == GetState.Found;
        PooledBufferWriter output = ObjectPools.RentWriter();
        try
        {
            ReadOnlySpan<byte> baseValue = !hasBase ? default : ValueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(Value.WrittenSpan) : Value.WrittenSpan;
            MergeHelper.TimedFullMerge(MergeOperator, userKey, hasBase, baseValue, MergeContext, output);
            Value.Clear();
            Value.Write(output.WrittenSpan);
        }
        finally
        {
            ObjectPools.ReturnWriter(output);
        }
        State = GetState.Found;
        ValueIsEntity = false;
        MergeContext.Clear();
        result = Value.WrittenSpan;
        return true;
    }

    /// <summary>
    /// GetEntity's final step (DBImpl::GetEntity / GetContext with PinnableWideColumns): writes the key's serialized
    /// entity to <paramref name="entity"/>. A plain value becomes an entity with only the default column; merge operands
    /// are applied to the default column (MergeHelper::TimedFullMergeWithEntity) and the other columns are kept.
    /// Returns false if the key does not exist.
    /// </summary>
    public bool TryResolveEntity(ReadOnlySpan<byte> userKey, IBufferWriter<byte> entity)
    {
        switch (State)
        {
            case GetState.Corrupt:
                ThrowHelper.Corruption("corrupted entry type for key");
                return false;
        }
        if (NeedsBlobResolution) ThrowUnresolvedBlob();
        if (MergeContext.Count == 0)
        {
            if (State != GetState.Found) return false;
            if (ValueIsEntity) entity.Write(Value.WrittenSpan);
            else Wide.WideColumnSerialization.SerializeDefaultColumn(Value.WrittenSpan, entity);
            return true;
        }
        if (MergeOperator is null) throw RocksDbException.InvalidArgument("merge_operator is not properly initialized.");
        bool hasBase = State == GetState.Found;
        PooledBufferWriter output = ObjectPools.RentWriter();
        try
        {
            ReadOnlySpan<byte> baseValue = !hasBase ? default : ValueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(Value.WrittenSpan) : Value.WrittenSpan;
            MergeHelper.TimedFullMerge(MergeOperator, userKey, hasBase, baseValue, MergeContext, output);
            if (hasBase && ValueIsEntity) Wide.WideColumnSerialization.ReplaceDefaultColumn(Value.WrittenSpan, output.WrittenSpan, entity);
            else Wide.WideColumnSerialization.SerializeDefaultColumn(output.WrittenSpan, entity);
        }
        finally
        {
            ObjectPools.ReturnWriter(output);
        }
        return true;
    }

    /// <summary>
    /// After a successful <see cref="TryResolve"/>: replaces a found entity in <see cref="Value"/> with the value of its
    /// default column, for callers that read <see cref="Value"/> directly (MultiGet).
    /// </summary>
    public void ReplaceEntityWithDefaultColumn()
    {
        if (!ValueIsEntity || State != GetState.Found) return;
        Scratch.Clear();
        Scratch.Write(Wide.WideColumnSerialization.GetValueOfDefaultColumn(Value.WrittenSpan));
        Value.Clear();
        Value.Write(Scratch.WrittenSpan);
        ValueIsEntity = false;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowUnresolvedBlob() =>
        throw new InvalidOperationException("Internal error: a blob index was not resolved by the read path (GetContext.ResolveBlob).");

    // ---- pooling ----------------------------------------------------------------------------------------

    [ThreadStatic] private static GetContext? t_cached;
    private static readonly GetContext?[] s_pool = new GetContext?[Math.Min(64, Environment.ProcessorCount * 4)];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static GetContext Rent(MergeOperator? mergeOperator)
    {
        GetContext? c = t_cached;
        if (c is not null) t_cached = null;
        else c = RentShared();
        c.Reset(mergeOperator);
        return c;
    }

    private static GetContext RentShared()
    {
        GetContext?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            GetContext? c = pool[i];
            if (c is not null && Interlocked.CompareExchange(ref pool[i], null, c) == c) return c;
        }
        return new GetContext();
    }

    public static void Return(GetContext c)
    {
        c.MergeOperator = null;
        if (c.Value.Capacity > 1 << 20 || c.MergeContext.Count > 64)
        {
            return; // let oversized contexts go
        }
        if (t_cached is null)
        {
            t_cached = c;
            return;
        }
        GetContext?[] pool = s_pool;
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null && Interlocked.CompareExchange(ref pool[i], c, null) is null) return;
        }
    }
}
