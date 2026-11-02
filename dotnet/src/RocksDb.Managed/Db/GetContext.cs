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
/// TODO(merge): consolidate the final merge step with agent I's MergeHelper (db/merge_helper.cc TimedFullMerge).
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
    private readonly PooledBufferWriter _operandScratch = new(256);
    private int[] _offsets = new int[9];

    public GetState State;
    public ulong MaxCoveringTombstoneSeq;
    public MergeOperator? MergeOperator;

    public void Reset(MergeOperator? mergeOperator)
    {
        MergeContext.Clear();
        Value.Clear();
        State = GetState.NotFound;
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
                Value.Clear();
                Value.Write(value);
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
            case EntryType.WideColumnEntity:
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
                if (valueType is EntryType.BlobIndex or EntryType.WideColumnEntity)
                {
                    State = GetState.UnexpectedBlobIndex;
                    return true;
                }
                Value.Clear();
                Value.Write(value);
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
                ThrowHelper.NotSupported("Encounter unexpected blob index / wide-column entity. Blob files and wide columns are not supported yet.");
                return false;
        }
        if (MergeContext.Count == 0)
        {
            if (State != GetState.Found) return false;
            result = Value.WrittenSpan;
            return true;
        }
        // Merge operands present: combine with the base value (if found) or with no base (deleted / nothing older).
        if (MergeOperator is null) throw RocksDbException.InvalidArgument("merge_operator is not properly initialized.");
        bool hasBase = State == GetState.Found;
        int n = MergeContext.Count;
        if (_offsets.Length < n + 1) _offsets = new int[Math.Max(n + 1, _offsets.Length * 2)];
        _operandScratch.Clear();
        _offsets[0] = 0;
        for (int i = 0; i < n; i++)
        {
            _operandScratch.Write(MergeContext.GetOperand(i)); // oldest first
            _offsets[i + 1] = _operandScratch.WrittenCount;
        }
        var operands = new MergeOperands(_operandScratch.WrittenSpan, _offsets.AsSpan(0, n + 1));
        PooledBufferWriter output = ObjectPools.RentWriter();
        try
        {
            if (!MergeOperator.FullMerge(userKey, hasBase, hasBase ? Value.WrittenSpan : default, operands, output))
            {
                throw new RocksDbException(StatusCode.Corruption, "Corruption: Error: Could not perform merge.", StatusSubCode.MergeOperatorFailed);
            }
            Value.Clear();
            Value.Write(output.WrittenSpan);
        }
        finally
        {
            ObjectPools.ReturnWriter(output);
        }
        State = GetState.Found;
        MergeContext.Clear();
        result = Value.WrittenSpan;
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
