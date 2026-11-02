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
        UnexpectedBlobIndex,
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
    public ulong MaxCoveringTombstoneSeq;
    public MergeOperator? MergeOperator;

    public void Reset(MergeOperator? mergeOperator)
    {
        MergeContext.Clear();
        Value.Clear();
        State = GetState.NotFound;
        FromMemTable = false;
        ValueIsEntity = false;
        MaxCoveringTombstoneSeq = 0;
        MergeOperator = mergeOperator;
    }

    /// <summary>
    /// Port of GetContext::SaveValue for one entry of a table file (user key already matched by the reader, sequence
    /// visible to the lookup). Returns true to keep scanning older entries of the same key.
    /// </summary>
    public bool SaveValue(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
    {
        DbFormat.UnpackSequenceAndType(DbFormat.ExtractFooter(internalKey), out ulong seq, out EntryType type);
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
                Value.Clear();
                Value.Write(value);
                ValueIsEntity = type == EntryType.WideColumnEntity;
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
            case EntryType.BlobIndex:
                State = GetState.UnexpectedBlobIndex;
                return false;
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
                if (valueType is EntryType.BlobIndex)
                {
                    State = GetState.UnexpectedBlobIndex;
                    return true;
                }
                Value.Clear();
                Value.Write(value);
                ValueIsEntity = valueType == EntryType.WideColumnEntity;
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

    public bool Done => State is GetState.Found or GetState.Deleted or GetState.Corrupt or GetState.UnexpectedBlobIndex;

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
            case GetState.UnexpectedBlobIndex:
                ThrowHelper.NotSupported("Encounter unexpected blob index. Blob files are not supported yet.");
                return false;
        }
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
            case GetState.UnexpectedBlobIndex:
                ThrowHelper.NotSupported("Encounter unexpected blob index. Blob files are not supported yet.");
                return false;
        }
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
