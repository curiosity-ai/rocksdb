using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed.Table;

/// <summary>
/// The keys of a batched table lookup (<see cref="BlockBasedTableReader.MultiGetAsync"/>) and the sink of the entries
/// found for them. Keys are addressed by an index chosen by the implementation.
/// </summary>
internal interface ITableMultiGetBatch
{
    /// <summary>The lookup key (user key + packed (snapshot sequence, kValueTypeForSeek)) of key <paramref name="key"/>.</summary>
    ReadOnlySpan<byte> InternalKey(int key);

    /// <summary><see cref="InternalKey"/> as memory (stable during the lookup; used across awaits).</summary>
    ReadOnlyMemory<byte> InternalKeyMemory(int key);

    /// <summary>
    /// Called for each entry with the user key of <paramref name="key"/> at or after its lookup key, newest first, until
    /// it returns false (like <see cref="TableEntryVisitor{TState}"/>).
    /// </summary>
    bool Visit(int key, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value);
}

public sealed partial class BlockBasedTableReader
{
    /// <summary>
    /// Batched point lookup. Port of BlockBasedTable::MultiGet (table/block_based/block_based_table_reader_sync_and_async.h):
    /// the filter is checked for the whole batch first, then the keys that may be present are looked up in key order
    /// with one index cursor, and consecutive keys that fall into the same data block share one block lookup (one
    /// cache probe / read instead of one per key).
    /// <para>
    /// <paramref name="keys"/>[0..<paramref name="count"/>) are key indices of <paramref name="batch"/> sorted by internal
    /// key; <paramref name="mayMatch"/> is scratch of at least <paramref name="count"/> entries. With
    /// <paramref name="synchronous"/> the returned task is always complete; otherwise data blocks missing from the block
    /// cache are read with async I/O.
    /// </para>
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    internal override async ValueTask MultiGetAsync<TBatch>(TBatch batch, int[] keys, int count, bool[] mayMatch, TableReadFlags flags, bool synchronous,
        CancellationToken cancellationToken)
    {
        int candidates = FilterBatch(batch, keys, count, mayMatch, flags);
        if (candidates == 0) return;
        IndexCursor idx = IndexCursor.Rent(this, flags);
        BlockIter data = IterPool.Rent();
        BlockRef current = default;
        ulong currentOffset = ulong.MaxValue;
        try
        {
            for (int i = 0; i < count; i++)
            {
                if (!mayMatch[i]) continue;
                int key = keys[i];
                if (synchronous)
                {
                    SeekIndex(idx, batch, key);
                }
                else
                {
                    // Index partitions that are not cached are read with async I/O too.
                    ValueTask seek = idx.SeekForGetAsync(batch.InternalKeyMemory(key), cancellationToken);
                    if (!seek.IsCompletedSuccessfully) await seek.ConfigureAwait(false);
                }
                bool first = true;
                while (idx.Valid)
                {
                    BlockHandle handle = idx.Handle;
                    if (handle.Offset != currentOffset)
                    {
                        // A new data block: the previous one is not needed by later (larger) keys.
                        if (!current.IsEmpty) current.Release();
                        current = default;
                        currentOffset = ulong.MaxValue;
                        if (synchronous)
                        {
                            current = GetBlockRef(handle, BlockKind.Data, flags);
                        }
                        else
                        {
                            ValueTask<BlockRef> pending = GetBlockRefAsync(handle, BlockKind.Data, flags, cancellationToken);
                            current = pending.IsCompletedSuccessfully ? pending.Result : await pending.ConfigureAwait(false);
                        }
                        currentOffset = handle.Offset;
                    }
                    int r = VisitBlock(batch, key, data, current.Block!, first);
                    if (r <= 0) break; // done with this key
                    first = false;
                    if (synchronous)
                    {
                        idx.Next();
                    }
                    else
                    {
                        ValueTask next = idx.NextAsync(cancellationToken);
                        if (!next.IsCompletedSuccessfully) await next.ConfigureAwait(false);
                    }
                }
            }
        }
        finally
        {
            if (!current.IsEmpty) current.Release();
            IterPool.Return(data);
            idx.Return();
        }
    }

    private int FilterBatch<TBatch>(TBatch batch, int[] keys, int count, bool[] mayMatch, TableReadFlags flags)
        where TBatch : class, ITableMultiGetBatch
    {
        int candidates = 0;
        for (int i = 0; i < count; i++)
        {
            bool m = InternalKeyMayMatch(batch.InternalKey(keys[i]), flags);
            mayMatch[i] = m;
            if (m) candidates++;
        }
        return candidates;
    }

    private static void SeekIndex<TBatch>(IndexCursor idx, TBatch batch, int key) where TBatch : class, ITableMultiGetBatch =>
        idx.SeekForGet(batch.InternalKey(key));

    /// <summary>
    /// Visits the entries of <paramref name="key"/> in <paramref name="block"/>. Returns 1 when the key's entries may
    /// continue in the next block, 0 (or -1) when the key is done.
    /// </summary>
    private int VisitBlock<TBatch>(TBatch batch, int key, BlockIter data, Block block, bool first) where TBatch : class, ITableMultiGetBatch
    {
        ReadOnlySpan<byte> internalKey = batch.InternalKey(key);
        data.InitializeData(block, _icmp, GlobalSequenceNumber, PadTimestampSize);
        try
        {
            if (first)
            {
                if (!SeekForGet(data, internalKey)) return -1;
            }
            else
            {
                data.SeekToFirst();
            }
            ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(internalKey);
            for (; data.Valid; data.Next())
            {
                ReadOnlySpan<byte> k = data.Key;
                if (!UserKeyEquals(DbFormat.ExtractUserKey(k), userKey)) return 0;
                if (!batch.Visit(key, k, data.Value)) return 0;
            }
            return 1;
        }
        finally
        {
            data.Clear();
        }
    }
}
