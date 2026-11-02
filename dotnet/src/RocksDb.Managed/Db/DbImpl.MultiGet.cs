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
        Statistics? st = Stats;
        long t0 = st is null ? 0 : st.StartTimer();
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        ulong snapshot = ReadSequence(options);
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
            int n = batch.Count;
            int[] order = batch.Order;
            bool memtableOnly = options?.ReadTier == ReadTier.MemtableTier;

            // Memtables (MultiGetImpl: per key, newest memtable first). Keys still pending stay in sorted order.
            int pending = 0;
            for (int i = 0; i < n; i++)
            {
                int k = order[i];
                if (GetFromMemTables(sv, batch.UserKey(k), snapshot, batch.Context(k))) continue;
                if (memtableOnly)
                {
                    if (batch.Context(k).State != GetContext.GetState.NotFound)
                        batch.SetError(k, new RocksDbException(StatusCode.Incomplete, "Incomplete: merge operands not all in memtables"));
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
                for (int fi = 0; fi < l0.Length && pending > 0; fi++)
                {
                    FileMetaData f = l0[fi];
                    int m = 0;
                    for (int j = 0; j < pending; j++)
                    {
                        int k = order[j];
                        if (UserKeyInFileRange(ucmp, batch.UserKey(k), f)) sub[m++] = k;
                    }
                    if (m == 0) continue;
                    await MultiGetFromFileAsync(cfd, f, batch, m, snapshot, synchronous, cancellationToken).ConfigureAwait(false);
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
                        await MultiGetFromFileAsync(cfd, f, batch, m, snapshot, synchronous, cancellationToken).ConfigureAwait(false);
                    }
                    pending = RemoveDone(batch, order, pending);
                }
            }
            // Blob indexes are resolved while the super version (and so its blob files) is referenced.
            for (int k = 0; k < n; k++)
            {
                GetContext ctx = batch.Context(k);
                if (!ctx.NeedsBlobResolution) continue;
                if (synchronous) ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, batch.UserKey(k));
                else await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, batch.UserKeyMemory(k), cancellationToken).ConfigureAwait(false);
            }
            batch.FinishRun();
            if (st is not null) RecordMultiGet(st, batch, t0);
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>MultiGet statistics (DBImpl::MultiGetCommon: calls, keys read/found, bytes read, latency).</summary>
    private static void RecordMultiGet(Statistics st, MultiGetBatch batch, long startTimestamp)
    {
        ulong bytes = 0;
        for (int i = 0; i < batch.Count; i++) bytes += (ulong)batch.GetValue(i).Length;
        st.RecordTick(Ticker.NumberMultigetCalls);
        st.RecordTick(Ticker.NumberMultigetKeysRead, (ulong)batch.Count);
        st.RecordTick(Ticker.NumberMultigetKeysFound, (ulong)batch.FoundCount);
        st.RecordTick(Ticker.NumberMultigetBytesRead, bytes);
        st.RecordInHistogram(Histogram.BytesPerMultiget, bytes);
        st.RecordElapsedMicros(Histogram.DbMultiget, startTimestamp);
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
        TableReaderLease lease = cfd.TableCache.GetReader(f, out FragmentedRangeTombstoneList? rangeDels);
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
            t = lease.Reader.MultiGetAsync(batch, sub, count, batch.MayMatch, TableReadFlags.None, synchronous, cancellationToken);
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
