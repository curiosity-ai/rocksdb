using System.Runtime.CompilerServices;
using RocksDb.Managed.Table;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    /// <summary>
    /// Batched point lookup of the keys of <paramref name="batch"/>. Port of DBImpl::MultiGetImpl +
    /// Version::MultiGet (db/db_impl/db_impl.cc, db/version_set.cc): one super version and one read sequence for the
    /// whole batch, keys sorted by user key, memtables searched per key, then every table file that may hold some of
    /// the remaining keys is visited once with the sub-batch of keys in its range (L0 files newest first, then one
    /// file per key and level, consecutive keys of the same file grouped). Table lookups go through
    /// <see cref="BlockBasedTableReader.MultiGetAsync"/>. With <paramref name="synchronous"/> the returned task is
    /// always complete; otherwise data blocks missing from the block cache are read with async I/O.
    /// Per-key outcomes are recorded in <paramref name="batch"/>; I/O errors fail the whole call.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask MultiGetAsync(ColumnFamilyData cfd, ReadOptions? options, MultiGetBatch batch, bool synchronous, CancellationToken cancellationToken)
    {
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        if (_txnHooks is not null)
        {
            // WritePreparedTxnDB::MultiGet: one GetImpl (with its read callback) per key.
            MultiGetWithTxnHooks(cfd, options, batch);
            return;
        }
        if (_queryTracer is { } tracer) TraceMultiGet(tracer, cfd, batch, options);
        if (BlockCacheTracer.IsEnabled) SetBlockCacheTraceContext(Tracing.TableReaderCaller.UserMultiGet, default, options);
        Statistics? st = Stats;
        long t0 = st is null ? 0 : st.StartTimer();
        SuperVersion sv = GetAndRefSuperVersion(cfd, out int svSlot);
        ulong snapshot = ReadSequence(options);
        // ReadOptions::deadline: file reads of a synchronous run check it (the thread-local deadline must not leak to
        // other work on the threads an async run continues on).
        long deadlineToken = synchronous ? ReadDeadline.Enter(options) : -1;
        try
        {
            if (cfd.TimestampSize != 0)
            {
                if (synchronous) MultiGetWithTimestamp(cfd, sv, snapshot, options, batch);
                else await MultiGetWithTimestampAsync(cfd, sv, snapshot, options, batch, cancellationToken).ConfigureAwait(false);
                if (st is not null) RecordMultiGet(st, batch, t0);
                return;
            }
            Comparator ucmp = cfd.UserComparator;
            batch.BeginRun(cfd.MergeOperator, snapshot, ucmp);
            batch.ReadFlags = TableReadFlagsExtensions.FromReadOptions(options);
            int n = batch.Count;
            if (ReadDeadline.IsExceeded(options))
            {
                // MultiGetImpl: the deadline is checked before each batch of keys; the keys left are TimedOut.
                for (int k = 0; k < n; k++) batch.SetTimedOut(k);
                batch.FinishRun();
                if (st is not null) RecordMultiGet(st, batch, t0);
                return;
            }
            int[] order = batch.Order;
            // MultiGetImpl skip_memtable: kPersistedTier reads skip the memtables once data was written without the
            // WAL. (kMemtableTier does not restrict MultiGet: like Get, it reads the table files.)
            bool skipMemTables = SkipMemTables(options);
            // value_size_soft_limit: once the values found so far exceed it, the keys not yet done are Aborted.
            ulong softLimit = options?.ValueSizeSoftLimit ?? ulong.MaxValue;
            ulong valueSize = 0;

            // Memtables (MultiGetImpl: per key, newest memtable first). Keys still pending stay in sorted order.
            int pending = 0;
            for (int i = 0; i < n; i++)
            {
                int k = order[i];
                if (!skipMemTables && GetFromMemTables(sv, batch.UserKey(k), snapshot, batch.Context(k)))
                {
                    if (softLimit != ulong.MaxValue && AddValueSize(batch, k, ref valueSize, softLimit))
                    {
                        // MemTable::MultiGet: every key still in the range is Aborted.
                        for (int j = i + 1; j < n; j++) batch.SetAborted(order[j]);
                        for (int j = 0; j < pending; j++) batch.SetAborted(order[j]);
                        pending = 0;
                        break;
                    }
                    continue;
                }
                order[pending++] = k;
            }

            if (pending > 0)
            {
                VersionStorageInfo s = sv.Current.Storage;
                int[] sub = batch.Sub;
                // L0: every file may overlap every key; newest first.
                FileMetaData[] l0 = s.LevelFilesArray(0);
                bool checkL0Range = CheckL0KeyRange(s, l0.Length); // FilePickerMultiGet
                for (int fi = 0; fi < l0.Length && pending > 0; fi++)
                {
                    FileMetaData f = l0[fi];
                    int m = 0;
                    for (int j = 0; j < pending; j++)
                    {
                        int k = order[j];
                        if (!checkL0Range || UserKeyInFileRange(ucmp, batch.UserKey(k), f)) sub[m++] = k;
                    }
                    if (m == 0) continue;
                    batch.ReadFlags = WithFilterSkip(cfd, s, 0, fi == l0.Length - 1, batch.ReadFlags);
                    await MultiGetFromFileAsync(cfd, f, batch, m, snapshot, synchronous, cancellationToken).ConfigureAwait(false);
                    SampleMultiGetReads(f, batch, m);
                    MarkHitLevel(batch, m, 0);
                    if (softLimit != ulong.MaxValue && ApplySoftLimit(batch, m, ref valueSize, softLimit)) AbortPending(batch, order, pending);
                    pending = RemoveDone(batch, order, pending);
                }
                // Sorted levels: one file per key (FindFile on the lookup key); consecutive keys of a file are grouped.
                for (int level = 1; level < s.NumLevels && pending > 0; level++)
                {
                    if (s.NumLevelFiles(level) == 0) continue;
                    FileMetaData[] files = s.LevelFilesArray(level);
                    int j = 0;
                    while (j < pending)
                    {
                        int idx = s.FindFile(level, batch.InternalKey(order[j]));
                        if (idx >= files.Length) break; // this and all larger keys are past the last file
                        FileMetaData f = files[idx];
                        int m = 0;
                        for (; j < pending; j++)
                        {
                            int k = order[j];
                            if (cfd.Icmp.Compare(batch.InternalKey(k), f.Largest) > 0) break;
                            if (ucmp.Compare(batch.UserKey(k), DbFormat.ExtractUserKey(f.Smallest)) >= 0) sub[m++] = k;
                        }
                        if (m == 0) continue;
                        batch.ReadFlags = WithFilterSkip(cfd, s, level, false, batch.ReadFlags);
                        bool aborted = false;
                        // FilePickerMultiGet: a key that is the file's largest user key and not done yet continues in
                        // the next file of the level (its versions can span adjacent files; see GetFromVersion).
                        for (int fileIdx = idx; ; )
                        {
                            await MultiGetFromFileAsync(cfd, f, batch, m, snapshot, synchronous, cancellationToken).ConfigureAwait(false);
                            SampleMultiGetReads(f, batch, m);
                            MarkHitLevel(batch, m, level);
                            if (softLimit != ulong.MaxValue && ApplySoftLimit(batch, m, ref valueSize, softLimit))
                            {
                                aborted = true;
                                break;
                            }
                            if (++fileIdx >= files.Length) break;
                            FileMetaData next = files[fileIdx];
                            int carried = 0;
                            for (int q = 0; q < m; q++)
                            {
                                int k = sub[q];
                                if (!batch.Context(k).Done && ucmp.Compare(batch.UserKey(k), DbFormat.ExtractUserKey(f.Largest)) == 0 &&
                                    ucmp.Compare(batch.UserKey(k), DbFormat.ExtractUserKey(next.Smallest)) >= 0)
                                {
                                    sub[carried++] = k;
                                }
                            }
                            if (carried == 0) break;
                            f = next;
                            m = carried;
                        }
                        if (aborted)
                        {
                            AbortPending(batch, order, pending);
                            break;
                        }
                    }
                    pending = RemoveDone(batch, order, pending);
                }
            }
            // Blob indexes are resolved while the super version (and so its blob files) is referenced.
            for (int k = 0; k < n; k++)
            {
                GetContext ctx = batch.Context(k);
                if (!ctx.NeedsBlobResolution) continue;
                try
                {
                    if (synchronous) ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, batch.UserKey(k));
                    else await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, batch.UserKeyMemory(k), cancellationToken).ConfigureAwait(false);
                }
                catch (RocksDbException e) when (e.Code == StatusCode.Incomplete)
                {
                    // Version::MultiGetBlob with kBlockCacheTier: a blob that is not cached makes only this key Incomplete.
                    batch.SetError(k, e);
                }
            }
            batch.FinishRun();
            if (st is not null) RecordMultiGet(st, batch, t0);
        }
        finally
        {
            ReadDeadline.Exit(deadlineToken);
            ReturnAndCleanupSuperVersion(cfd, sv, svSlot);
        }
    }

    /// <summary>
    /// MultiGetRange::AddValueSize for key <paramref name="k"/> found with a value; true when the total now exceeds
    /// <paramref name="limit"/> (read_options.value_size_soft_limit). Deviation: a value still to be merged counts
    /// with the size of its base value (upstream adds the merged size).
    /// </summary>
    private static bool AddValueSize(MultiGetBatch batch, int k, ref ulong valueSize, ulong limit)
    {
        GetContext ctx = batch.Context(k);
        if (ctx.State != GetContext.GetState.Found || batch.GetStatus(k) == MultiGetStatus.Error) return false;
        valueSize += (ulong)ctx.Value.WrittenCount;
        return valueSize > limit;
    }

    /// <summary>
    /// The value_size_soft_limit check of Version::MultiGetFromSST for the keys batch.Sub[0..count) just looked up in
    /// one file (in key order): the key whose value crosses the limit is returned, later keys found in the file are
    /// Aborted. True when the limit was crossed.
    /// </summary>
    private static bool ApplySoftLimit(MultiGetBatch batch, int count, ref ulong valueSize, ulong limit)
    {
        int[] sub = batch.Sub;
        bool exceeded = valueSize > limit;
        for (int i = 0; i < count; i++)
        {
            int k = sub[i];
            if (exceeded)
            {
                if (batch.Context(k).State == GetContext.GetState.Found) batch.SetAborted(k);
                continue;
            }
            exceeded = AddValueSize(batch, k, ref valueSize, limit);
        }
        return exceeded;
    }

    /// <summary>Version::MultiGet after Aborted: every key whose lookup is not finished is Aborted.</summary>
    private static void AbortPending(MultiGetBatch batch, int[] order, int pending)
    {
        for (int i = 0; i < pending; i++)
        {
            int k = order[i];
            if (!batch.Context(k).Done) batch.SetAborted(k);
        }
    }

    /// <summary>Records the level at which the keys batch.Sub[0..count) finished their lookup (hit_file_level).</summary>
    private static void MarkHitLevel(MultiGetBatch batch, int count, int level)
    {
        int[] sub = batch.Sub;
        for (int i = 0; i < count; i++)
        {
            GetContext ctx = batch.Context(sub[i]);
            if (ctx.Done && ctx.HitLevel < 0) ctx.HitLevel = level;
        }
    }

    /// <summary>
    /// MultiGet statistics (DBImpl::MultiGetCommon: calls, keys read/found, bytes read, latency) and, per key found in a
    /// table file, GET_HIT_L0 / GET_HIT_L1 / GET_HIT_L2_AND_UP (Version::MultiGetFromSST, kFound).
    /// </summary>
    private static void RecordMultiGet(Statistics st, MultiGetBatch batch, long startTimestamp)
    {
        ulong bytes = 0;
        for (int i = 0; i < batch.Count; i++)
        {
            bytes += (ulong)batch.GetValue(i).Length;
            GetContext ctx = batch.Context(i);
            if (ctx.HitLevel >= 0 && ctx.State == GetContext.GetState.Found)
            {
                st.RecordTick(ctx.HitLevel switch { 0 => Ticker.GetHitL0, 1 => Ticker.GetHitL1, _ => Ticker.GetHitL2AndUp });
            }
        }
        st.RecordTick(Ticker.NumberMultigetCalls);
        st.RecordTick(Ticker.NumberMultigetKeysRead, (ulong)batch.Count);
        st.RecordTick(Ticker.NumberMultigetKeysFound, (ulong)batch.FoundCount);
        st.RecordTick(Ticker.NumberMultigetBytesRead, bytes);
        st.RecordInHistogram(Histogram.BytesPerMultiget, bytes);
        st.RecordElapsedMicros(Histogram.DbMultiget, startTimestamp);
    }

    /// <summary>
    /// The read sampling of Version::MultiGetFromSST (version_set_sync_and_async.h) for the keys batch.Sub[0..count) just
    /// looked up in <paramref name="f"/>. Upstream decides once per key (GetContext::sample_); a fresh draw per key and
    /// file has the same rate.
    /// </summary>
    private static void SampleMultiGetReads(FileMetaData f, MultiGetBatch batch, int count)
    {
        int[] sub = batch.Sub;
        for (int i = 0; i < count; i++)
        {
            if (!FileReadSample.ShouldSampleFileRead()) continue;
            FileReadSample.SampleFileReadInc(f);
            SampleCollapsibleRead(f, batch.Context(sub[i]));
        }
    }

    /// <summary>Drops the keys whose lookup finished from the front <paramref name="pending"/> entries of <paramref name="order"/>.</summary>
    private static int RemoveDone(MultiGetBatch batch, int[] order, int pending)
    {
        int w = 0;
        for (int i = 0; i < pending; i++)
        {
            int k = order[i];
            if (!batch.Context(k).Done) order[w++] = k;
        }
        return w;
    }

    /// <summary>TableCache::MultiGet: range tombstones of the file for each key, then the batched table lookup of batch.Sub[0..count).</summary>
    private static ValueTask MultiGetFromFileAsync(ColumnFamilyData cfd, FileMetaData f, MultiGetBatch batch, int count, ulong snapshot, bool synchronous,
        CancellationToken cancellationToken)
    {
        if (cfd.TableCache.RowCache is not null) return MultiGetFromFileWithRowCacheAsync(cfd, f, batch, count, snapshot, synchronous, cancellationToken);
        return MultiGetFromFileCoreAsync(cfd, f, batch, count, snapshot, synchronous, cancellationToken);
    }

    /// <summary>
    /// TableCache::MultiGet with a row cache: keys with a cached entry for the file are replayed (ROW_CACHE_HIT), the
    /// others (ROW_CACHE_MISS) are looked up in the table together and their visited entries cached.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private static async ValueTask MultiGetFromFileWithRowCacheAsync(ColumnFamilyData cfd, FileMetaData f, MultiGetBatch batch, int count, ulong snapshot,
        bool synchronous, CancellationToken cancellationToken)
    {
        TableCache tc = cfd.TableCache;
        Cache.RowCache rc = tc.RowCache!;
        int[] sub = batch.Sub;
        int[] original = System.Buffers.ArrayPool<int>.Shared.Rent(count);
        Util.PooledBufferWriter?[] logs = System.Buffers.ArrayPool<Util.PooledBufferWriter?>.Shared.Rent(count);
        byte[] keyBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(256);
        int misses = 0;
        try
        {
            Array.Copy(sub, original, count);
            for (int i = 0; i < count; i++)
            {
                int k = original[i];
                ReadOnlySpan<byte> userKey = batch.UserKey(k);
                if (keyBuf.Length < userKey.Length + TableCache.RowCacheKeyPrefixMax)
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(keyBuf);
                    keyBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(userKey.Length + TableCache.RowCacheKeyPrefixMax);
                }
                int n = tc.RowCacheKey(keyBuf, f, userKey, snapshot, out ulong replaySeq);
                if (rc.TryGet(keyBuf.AsSpan(0, n), out byte[]? log))
                {
                    tc.Statistics?.RecordTick(Ticker.RowCacheHit);
                    batch.Context(k).Replay(log, userKey, replaySeq);
                    continue;
                }
                tc.Statistics?.RecordTick(Ticker.RowCacheMiss);
                Util.PooledBufferWriter w = Util.ObjectPools.RentWriter();
                logs[misses] = w;
                batch.Context(k).ReplayLog = w;
                sub[misses++] = k;
            }
            if (misses > 0)
            {
                try
                {
                    await MultiGetFromFileCoreAsync(cfd, f, batch, misses, snapshot, synchronous, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    for (int i = 0; i < misses; i++) batch.Context(sub[i]).ReplayLog = null;
                }
                for (int i = 0; i < misses; i++)
                {
                    Util.PooledBufferWriter w = logs[i]!;
                    if (w.WrittenCount == 0 || batch.GetStatus(sub[i]) == MultiGetStatus.Error) continue;
                    ReadOnlySpan<byte> userKey = batch.UserKey(sub[i]);
                    if (keyBuf.Length < userKey.Length + TableCache.RowCacheKeyPrefixMax)
                    {
                        System.Buffers.ArrayPool<byte>.Shared.Return(keyBuf);
                        keyBuf = System.Buffers.ArrayPool<byte>.Shared.Rent(userKey.Length + TableCache.RowCacheKeyPrefixMax);
                    }
                    int n = tc.RowCacheKey(keyBuf, f, userKey, snapshot, out _);
                    rc.Insert(keyBuf.AsSpan(0, n), w.WrittenSpan);
                }
            }
        }
        finally
        {
            for (int i = 0; i < misses; i++)
            {
                Util.ObjectPools.ReturnWriter(logs[i]!);
                logs[i] = null;
            }
            // The caller continues with batch.Sub[0..count) in key order.
            Array.Copy(original, sub, count);
            System.Buffers.ArrayPool<int>.Shared.Return(original);
            System.Buffers.ArrayPool<Util.PooledBufferWriter?>.Shared.Return(logs);
            System.Buffers.ArrayPool<byte>.Shared.Return(keyBuf);
        }
    }

    /// <summary>The table lookup of <see cref="MultiGetFromFileAsync"/>.</summary>
    private static ValueTask MultiGetFromFileCoreAsync(ColumnFamilyData cfd, FileMetaData f, MultiGetBatch batch, int count, ulong snapshot, bool synchronous,
        CancellationToken cancellationToken)
    {
        FragmentedRangeTombstoneList? rangeDels;
        TableReaderLease lease;
        try
        {
            lease = cfd.TableCache.GetReaderForVersion(f, out rangeDels, batch.ReadFlags);
        }
        catch (RocksDbException e) when (e.Code == StatusCode.Incomplete)
        {
            // TableCache::MultiGet with no_io and a table that is not open: the keys of this file are Incomplete.
            for (int i = 0; i < count; i++) batch.SetIncomplete(batch.Sub[i]);
            return ValueTask.CompletedTask;
        }
        ValueTask t;
        try
        {
            int[] sub = batch.Sub;
            if (rangeDels is not null)
            {
                for (int i = 0; i < count; i++)
                {
                    GetContext ctx = batch.Context(sub[i]);
                    ulong cov = rangeDels.MaxCoveringTombstoneSeqnum(batch.UserKey(sub[i]), snapshot);
                    if (cov > ctx.MaxCoveringTombstoneSeq) ctx.MaxCoveringTombstoneSeq = cov;
                }
            }
            t = lease.Reader is BlockBasedTableReader bbt
                ? bbt.MultiGetAsync(batch, sub, count, batch.MayMatch, batch.ReadFlags, synchronous, cancellationToken)
                : lease.Reader.MultiGetAsync(batch, sub, count, batch.MayMatch, batch.ReadFlags, synchronous, cancellationToken);
            if (t.IsCompleted)
            {
                lease.Dispose();
                return t;
            }
        }
        catch
        {
            lease.Dispose();
            throw;
        }
        return AwaitAndRelease(t, lease);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private static async ValueTask AwaitAndRelease(ValueTask t, TableReaderLease lease)
    {
        try
        {
            await t.ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    /// <summary>
    /// MultiGet on a column family with user-defined timestamps: every key is looked up on the same super version and
    /// sequence like the batched path, one key at a time (the batched table lookup is not timestamp-aware).
    /// </summary>
    /// <summary>
    /// Async <see cref="MultiGetWithTimestamp"/>: each key's table lookups read data blocks with async I/O
    /// (<see cref="GetWithSuperVersionAsync"/>).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask MultiGetWithTimestampAsync(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, MultiGetBatch batch,
        CancellationToken cancellationToken)
    {
        GetReadTimestamp(cfd, options);
        batch.BeginRun(cfd.MergeOperator, snapshot, cfd.UserComparator.RootComparator);
        for (int k = 0; k < batch.Count; k++)
        {
            try
            {
                await GetWithSuperVersionAsync(cfd, sv, snapshot, options, batch.UserKeyMemory(k), batch.Context(k), cancellationToken).ConfigureAwait(false);
            }
            catch (RocksDbException e)
            {
                batch.SetError(k, e);
            }
        }
        batch.FinishRun();
    }

    private void MultiGetWithTimestamp(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, MultiGetBatch batch)
    {
        GetReadTimestamp(cfd, options); // validate once (throws for the whole call, like GetImpl's checks in MultiGet)
        batch.BeginRun(cfd.MergeOperator, snapshot, cfd.UserComparator.RootComparator);
        for (int k = 0; k < batch.Count; k++)
        {
            try
            {
                GetWithSuperVersion(cfd, sv, snapshot, options, batch.UserKey(k), batch.Context(k));
            }
            catch (RocksDbException e)
            {
                batch.SetError(k, e);
            }
        }
        batch.FinishRun();
    }

    /// <summary>MultiGet of a WritePrepared / WriteUnprepared DB: point reads with the transaction DB's read callback.</summary>
    private void MultiGetWithTxnHooks(ColumnFamilyData cfd, ReadOptions? options, MultiGetBatch batch)
    {
        batch.BeginRun(cfd.MergeOperator, ReadSequence(options), cfd.UserComparator);
        for (int k = 0; k < batch.Count; k++)
        {
            try
            {
                Get(cfd, options, batch.UserKey(k), batch.Context(k));
            }
            catch (RocksDbException e)
            {
                batch.SetError(k, e);
            }
        }
        batch.FinishRun();
    }
}
