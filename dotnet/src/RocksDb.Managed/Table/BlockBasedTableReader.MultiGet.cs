using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Cache;
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
    /// <para>
    /// With a cache that has secondary tiers (<see cref="BlockCache.ExtendedLookup"/>), the first data block of every key
    /// is looked up up front: one StartAsyncLookup per distinct block, then a single WaitAll (WaitAllAsync when not
    /// <paramref name="synchronous"/>) for all of them, as upstream's MultiGet does, so the lookups a non-volatile tier
    /// does asynchronously overlap. Blocks that miss are then read from the file without a second cache lookup.
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
        MultiGetCacheLookups? lookups = null;
        try
        {
            if (_cache is { ExtendedLookup: true } extended)
            {
                lookups = MultiGetCacheLookups.Rent(candidates);
                for (int i = 0; i < count; i++)
                {
                    if (!mayMatch[i]) continue;
                    if (synchronous)
                    {
                        SeekIndex(idx, batch, keys[i]);
                    }
                    else
                    {
                        ValueTask seek = idx.SeekForGetAsync(batch.InternalKeyMemory(keys[i]), cancellationToken);
                        if (!seek.IsCompletedSuccessfully) await seek.ConfigureAwait(false);
                    }
                    if (idx.Valid) StartDataBlockLookup(extended, lookups, idx.Handle.Offset);
                }
                if (lookups.AnyPending)
                {
                    if (synchronous) extended.WaitAll(lookups.PendingSpan);
                    else await extended.WaitAllAsync(lookups.Handles, lookups.Count, cancellationToken).ConfigureAwait(false);
                }
                CollectDataBlockLookups(lookups, flags);
            }
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
                        if (lookups is not null && lookups.TryTake(handle.Offset, out BlockRef prefetched))
                        {
                            if (!prefetched.IsEmpty)
                            {
                                current = prefetched;
                            }
                            else if (synchronous)
                            {
                                // Missed every cache tier: read it (ReadAndInsert records the miss).
                                current = ReadAndInsert(_cache!, _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset), handle, BlockKind.Data, flags);
                            }
                            else
                            {
                                _stats?.Record(MissCounter(BlockKind.Data));
                                current = await ReadAndInsertAsync(_cache!, _baseCacheKey.WithOffsetForMinSizeRecord(handle.Offset), handle, BlockKind.Data, flags,
                                    cancellationToken).ConfigureAwait(false);
                            }
                        }
                        else if (synchronous)
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
            lookups?.Return();
            IterPool.Return(data);
            idx.Return();
        }
    }

    // StartAsyncLookupFull for the data block at `offset` unless it is the previous key's block (keys are sorted, so a
    // block shared by consecutive keys is looked up once).
    private void StartDataBlockLookup(BlockCache cache, MultiGetCacheLookups lookups, ulong offset)
    {
        if (lookups.Count > 0 && lookups.Offsets[lookups.Count - 1] == offset) return;
        AsyncLookupHandle h = lookups.Add(offset);
        h.Key = _baseCacheKey.WithOffsetForMinSizeRecord(offset);
        h.Helper = _helpers[(int)BlockKind.Data];
        h.CreateContext = this;
        h.Priority = CachePriority.Low;
        h.Statistics = _stats?.Owner;
        cache.StartAsyncLookup(h);
        if (h.IsPending) lookups.AnyPending = true;
    }

    // The results of the batched lookups: hits become the keys' blocks (and count as block cache hits).
    private void CollectDataBlockLookups(MultiGetCacheLookups lookups, TableReadFlags flags)
    {
        for (int j = 0; j < lookups.Count; j++)
        {
            CachedBlock h = lookups.Handles[j].TakeResult();
            if (!h.HasValue) continue;
            var block = Unsafe.As<Block>(h.Value)!;
            _stats?.Record(HitCounter(BlockKind.Data));
            if (_bcTracer is { IsEnabled: true } t) TraceAccess(t, h.Key, BlockKind.Data, block, hit: true, flags);
            lookups.Blocks[j] = BlockRef.FromCache(block, h);
        }
    }

    /// <summary>
    /// Scratch of the batched data block lookups of one MultiGet (the async_handles / cache_keys arrays of upstream's
    /// MultiGet): lookup handles, block offsets (ascending) and the blocks found. Pooled.
    /// </summary>
    private sealed class MultiGetCacheLookups
    {
        private static readonly ConcurrentBag<MultiGetCacheLookups> s_pool = new();

        public AsyncLookupHandle[] Handles = [];
        public ulong[] Offsets = [];
        public BlockRef[] Blocks = [];
        public int Count;
        public bool AnyPending;
        private int _next;

        public ReadOnlySpan<AsyncLookupHandle> PendingSpan => Handles.AsSpan(0, Count);

        public static MultiGetCacheLookups Rent(int capacity)
        {
            if (!s_pool.TryTake(out MultiGetCacheLookups? l)) l = new MultiGetCacheLookups();
            if (l.Handles.Length < capacity)
            {
                int n = Math.Max(capacity, 2 * l.Handles.Length);
                var handles = new AsyncLookupHandle[n];
                Array.Copy(l.Handles, handles, l.Handles.Length);
                for (int i = l.Handles.Length; i < n; i++) handles[i] = new AsyncLookupHandle();
                l.Handles = handles;
                l.Offsets = new ulong[n];
                l.Blocks = new BlockRef[n];
            }
            return l;
        }

        public AsyncLookupHandle Add(ulong offset)
        {
            AsyncLookupHandle h = Handles[Count];
            h.Reset();
            Offsets[Count] = offset;
            Count++;
            return h;
        }

        /// <summary>
        /// Takes the lookup result of the block at <paramref name="offset"/> (empty block: it missed every tier). False
        /// when the block was not looked up (a later block of a key whose entries continue).
        /// </summary>
        public bool TryTake(ulong offset, out BlockRef block)
        {
            while (_next < Count && Offsets[_next] < offset) _next++;
            if (_next < Count && Offsets[_next] == offset)
            {
                block = Blocks[_next];
                Blocks[_next] = default;
                _next++;
                return true;
            }
            block = default;
            return false;
        }

        public void Return()
        {
            for (int j = 0; j < Count; j++)
            {
                // Results never taken (e.g. an exception): release them.
                if (!Blocks[j].IsEmpty) Blocks[j].Release();
                Blocks[j] = default;
                CachedBlock stray = Handles[j].TakeResult();
                stray.Dispose();
                Handles[j].Reset();
            }
            Count = 0;
            _next = 0;
            AnyPending = false;
            s_pool.Add(this);
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
