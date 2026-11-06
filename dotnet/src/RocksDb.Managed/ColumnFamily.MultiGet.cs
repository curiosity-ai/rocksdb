using System.Buffers;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed;

// MultiGet with caller-owned keys and results: the managed form of
// DB::MultiGet(ReadOptions, ColumnFamilyHandle*, size_t num_keys, const Slice* keys, PinnableSlice* values,
// Status* statuses, bool sorted_input) (include/rocksdb/db.h), with the values copied into one caller buffer.
public sealed partial class ColumnFamily
{
    /// <summary>
    /// Looks up <paramref name="keys"/> from one consistent view (batched like DB::MultiGet, see
    /// <see cref="MultiGetBatch"/>) with every result in caller memory: the values of the found keys are appended to
    /// <paramref name="values"/> in key order, <c>valueRanges[i]</c> is the position of key <c>i</c>'s value relative to
    /// the first byte this call writes to <paramref name="values"/> (an empty range for a key that was not found or
    /// failed), and <c>statuses[i]</c> its outcome. A per-key failure (I/O error, corruption, Incomplete with
    /// <see cref="ReadTier.BlockCacheTier"/>, ...) is reported as <see cref="MultiGetStatus.Error"/> and does not stop
    /// the other keys; use a <see cref="MultiGetBatch"/> when the failure details are needed.
    /// <para>
    /// Uses a pooled batch internally: after warm-up the call does not allocate (beyond what <paramref name="values"/>
    /// allocates to grow).
    /// </para>
    /// </summary>
    /// <param name="options">Read options (null for the defaults).</param>
    /// <param name="keys">The keys to look up (duplicates allowed).</param>
    /// <param name="values">Receives the values of the found keys, back to back.</param>
    /// <param name="valueRanges">Per key: the value's range in the bytes written to <paramref name="values"/>; at least <c>keys.Length</c> long.</param>
    /// <param name="statuses">Per key: the outcome; at least <c>keys.Length</c> long.</param>
    /// <exception cref="ArgumentException"><paramref name="valueRanges"/> or <paramref name="statuses"/> is shorter than <paramref name="keys"/>.</exception>
    public void MultiGet(ReadOptions? options, ReadOnlySpan<ReadOnlyMemory<byte>> keys, IBufferWriter<byte> values, Span<Range> valueRanges,
        Span<MultiGetStatus> statuses)
    {
        ArgumentNullException.ThrowIfNull(values);
        CheckMultiGetOutputs(keys.Length, valueRanges.Length, statuses.Length);
        _db.ThrowIfDisposed();
        if (keys.IsEmpty) return;
        MultiGetBatch batch = MultiGetBatch.RentPooled(keys.Length);
        try
        {
            for (int i = 0; i < keys.Length; i++) batch.Add(keys[i].Span);
            MultiGet(options, batch);
            CopyMultiGetResults(batch, values, valueRanges, statuses);
        }
        finally
        {
            MultiGetBatch.ReturnPooled(batch);
        }
    }

    /// <summary>
    /// Async <see cref="MultiGet(ReadOptions?, ReadOnlySpan{ReadOnlyMemory{byte}}, IBufferWriter{byte}, Span{Range}, Span{MultiGetStatus})"/>:
    /// data blocks missing from the block cache are read with async I/O; completes synchronously when every block is
    /// cached. The caller must not touch <paramref name="values"/>, <paramref name="valueRanges"/> or
    /// <paramref name="statuses"/> until the returned task completes.
    /// </summary>
    public ValueTask MultiGetAsync(ReadOptions? options, ReadOnlyMemory<ReadOnlyMemory<byte>> keys, IBufferWriter<byte> values,
        Memory<Range> valueRanges, Memory<MultiGetStatus> statuses, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(values);
        CheckMultiGetOutputs(keys.Length, valueRanges.Length, statuses.Length);
        _db.ThrowIfDisposed();
        if (keys.IsEmpty) return ValueTask.CompletedTask;
        MultiGetBatch batch = MultiGetBatch.RentPooled(keys.Length);
        ValueTask run;
        try
        {
            ReadOnlySpan<ReadOnlyMemory<byte>> k = keys.Span;
            for (int i = 0; i < k.Length; i++) batch.Add(k[i].Span);
            run = MultiGetAsync(options, batch, cancellationToken);
            if (run.IsCompletedSuccessfully)
            {
                run.GetAwaiter().GetResult();
                CopyMultiGetResults(batch, values, valueRanges.Span, statuses.Span);
            }
        }
        catch
        {
            MultiGetBatch.ReturnPooled(batch);
            throw;
        }
        if (run.IsCompletedSuccessfully)
        {
            MultiGetBatch.ReturnPooled(batch);
            return ValueTask.CompletedTask;
        }
        return AwaitMultiGetAsync(run, batch, values, valueRanges, statuses);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private static async ValueTask AwaitMultiGetAsync(ValueTask run, MultiGetBatch batch, IBufferWriter<byte> values, Memory<Range> valueRanges,
        Memory<MultiGetStatus> statuses)
    {
        try
        {
            await run.ConfigureAwait(false);
            CopyMultiGetResults(batch, values, valueRanges.Span, statuses.Span);
        }
        finally
        {
            MultiGetBatch.ReturnPooled(batch);
        }
    }

    private static void CheckMultiGetOutputs(int keys, int ranges, int statuses)
    {
        if (ranges < keys) ThrowOutputTooShort("valueRanges");
        if (statuses < keys) ThrowOutputTooShort("statuses");
    }

    private static void ThrowOutputTooShort(string paramName) =>
        throw new ArgumentException("The output span must have at least one element per key.", paramName);

    private static void CopyMultiGetResults(MultiGetBatch batch, IBufferWriter<byte> values, Span<Range> valueRanges, Span<MultiGetStatus> statuses)
    {
        int written = 0;
        for (int i = 0; i < batch.Count; i++)
        {
            MultiGetStatus st = batch.GetStatus(i);
            statuses[i] = st;
            if (st != MultiGetStatus.Found)
            {
                valueRanges[i] = new Range(written, written);
                continue;
            }
            ReadOnlySpan<byte> v = batch.GetValue(i);
            if (!v.IsEmpty)
            {
                v.CopyTo(values.GetSpan(v.Length));
                values.Advance(v.Length);
            }
            valueRanges[i] = new Range(written, written + v.Length);
            written += v.Length;
        }
    }
}
