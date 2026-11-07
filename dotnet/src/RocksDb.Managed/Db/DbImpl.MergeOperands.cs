using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

/// <summary>
/// DB::GetMergeOperands: GetImpl in operand-collection mode (GetImplOptions::get_value = false, db/db_impl/db_impl.cc
/// PrepareGetMergeOperands / FinalizeGetMergeOperands; GetContext::SaveValue with do_merge_ = false and
/// MemTable::Get's continue_cb check).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// Collects the merge operands of <paramref name="key"/> (with the base value, if any, as the oldest operand) into
    /// <paramref name="result"/> in insertion order (oldest first).
    /// </summary>
    public void GetMergeOperands(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, GetMergeOperandsOptions mergeOptions,
        MergeOperandsResult result)
    {
        ArgumentNullException.ThrowIfNull(mergeOptions);
        ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        try
        {
            ctx.ContinueCallback = mergeOptions.ContinueCallback;
            Get(cfd, options, key, ctx);
            Finalize(ctx, mergeOptions, result);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>Async <see cref="GetMergeOperands"/>: table data blocks are read with async I/O.</summary>
    [System.Runtime.CompilerServices.AsyncMethodBuilder(typeof(System.Runtime.CompilerServices.PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask GetMergeOperandsAsync(ColumnFamilyData cfd, ReadOptions? options, ReadOnlyMemory<byte> key, GetMergeOperandsOptions mergeOptions,
        MergeOperandsResult result, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(mergeOptions);
        ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        try
        {
            ctx.ContinueCallback = mergeOptions.ContinueCallback;
            await GetAsync(cfd, options, key, ctx, ct).ConfigureAwait(false);
            Finalize(ctx, mergeOptions, result);
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>FinalizeGetMergeOperands: the operand count limit and the operands in insertion order.</summary>
    private static void Finalize(GetContext ctx, GetMergeOperandsOptions mergeOptions, MergeOperandsResult result)
    {
        result.Reset();
        if (ctx.State == GetContext.GetState.Corrupt) ThrowHelper.Corruption("corrupted entry type for key");
        MergeContext mc = ctx.MergeContext;
        // The base value is part of the operand list unless the lookup was ended by continue_cb at an operand.
        bool hasBase = ctx.State == GetContext.GetState.Found && !ctx.OperandsStopped;
        int count = mc.Count + (hasBase ? 1 : 0);
        if (count == 0) return; // NotFound
        result.Found = true;
        result.NumberOfOperands = count;
        if (count > mergeOptions.ExpectedMaxNumberOfOperands)
        {
            // Status::Incomplete(KMergeOperandsInsufficientCapacity): nothing is returned but the count.
            result.ExceededExpectedMax = true;
            return;
        }
        if (hasBase)
        {
            ReadOnlySpan<byte> v = ctx.Value.WrittenSpan;
            result.Add(ctx.ValueIsEntity ? Wide.WideColumnSerialization.GetValueOfDefaultColumn(v) : v);
        }
        for (int i = 0; i < mc.Count; i++) result.Add(mc.GetOperand(i));
    }
}
