using System.Runtime.CompilerServices;
using RocksDb.Managed.Db.Iterators;
using RocksDb.Managed.Table;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db;

internal sealed partial class DbImpl
{
    private static readonly TableEntryVisitor<GetContext> s_saveValue =
        static (ref GetContext ctx, ReadOnlySpan<byte> k, ReadOnlySpan<byte> v) => ctx.SaveValue(k, v);

    /// <summary>The sequence number a read with <paramref name="options"/> observes.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ulong ReadSequence(ReadOptions? options)
    {
        Snapshot? snap = options?.Snapshot;
        if (snap is null) return LastPublishedSequence;
        return snap.SequenceNumber;
    }

    /// <summary>Validates the snapshot of <paramref name="options"/> (before any reference is taken).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateReadSnapshot(ReadOptions? options)
    {
        Snapshot? snap = options?.Snapshot;
        if (snap is not null && snap.Owner != this) ThrowHelper.InvalidArgument("snapshot does not belong to this DB or was released");
    }

    /// <summary>
    /// Point lookup (DBImpl::GetImpl): mutable memtable, immutable memtables (newest first), then table files
    /// (Version::Get). On return <paramref name="ctx"/> holds the final state; use <see cref="GetContext.TryResolve"/>.
    /// </summary>
    public void Get(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        if (_txnHooks is not null && ctx.Callback is null)
        {
            GetWithTxnHooks(cfd, options, key, ctx);
            return;
        }
        if (_queryTracer is { } tracer) TraceGet(tracer, cfd, key, options);
        if (BlockCacheTracer.IsEnabled)
        {
            GetWithBlockCacheTrace(cfd, options, key, ctx);
            return;
        }
        if (options is not null && options.Deadline > TimeSpan.Zero)
        {
            GetWithDeadline(cfd, options, key, ctx);
            return;
        }
        GetImpl(cfd, options, key, ctx);
    }

    /// <summary>GetImpl with ReadOptions::deadline: table file reads after the deadline fail with TimedOut (<see cref="ReadDeadline"/>).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GetWithDeadline(ColumnFamilyData cfd, ReadOptions options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        long token = ReadDeadline.Enter(options);
        try
        {
            GetImpl(cfd, options, key, ctx);
        }
        finally
        {
            ReadDeadline.Exit(token);
        }
    }

    private void GetImpl(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetAndRefSuperVersion(cfd, out int svSlot);
        try
        {
            TestHooks.Invoke("DBImpl::GetImpl:AfterSuperVersion");
            ulong snapshot = ReadSequence(options);
            if (ctx.Callback is { } cb) snapshot = ApplyReadCallback(cb, options, snapshot);
            GetWithSuperVersion(cfd, sv, snapshot, options, key, ctx);
        }
        finally
        {
            ReturnAndCleanupSuperVersion(cfd, sv, svSlot);
        }
    }

    /// <summary>
    /// GetImpl while a block cache trace runs: the table lookups of this thread are attributed to a Get with a fresh
    /// get id (BlockCacheLookupContext{kUserGet, NextGetId()}).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void GetWithBlockCacheTrace(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        object? prev = Tracing.BlockCacheTracer.SetCaller(Tracing.TableReaderCaller.UserGet, BlockCacheTracer.NextGetId(), key,
            options?.Snapshot is not null);
        try
        {
            GetImpl(cfd, options, key, ctx);
        }
        finally
        {
            Tracing.BlockCacheTracer.RestoreCaller(prev);
        }
    }

    /// <summary>
    /// DBImpl::GetImpl with a read callback: with a snapshot the read sequence is the larger of the snapshot and the
    /// callback's max visible sequence (WriteUnprepared reads its own newer writes); without one the callback is
    /// refreshed with the latest sequence.
    /// </summary>
    private static ulong ApplyReadCallback(ReadCallback cb, ReadOptions? options, ulong snapshot)
    {
        if (options?.Snapshot is not null) return Math.Max(snapshot, cb.MaxVisibleSeq);
        cb.Refresh(snapshot);
        return snapshot;
    }

    /// <summary>
    /// A point read of a WritePrepared / WriteUnprepared DB through the plain API (WritePreparedTxnDB::GetImpl): the
    /// read uses the transaction DB's read callback and fails with TryAgain when its snapshot could not be validated.
    /// </summary>
    private void GetWithTxnHooks(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        SeqPerBatchHooks hooks = _txnHooks!;
        ReadCallback cb = hooks.BeginRead(options?.Snapshot);
        ctx.Callback = cb;
        try
        {
            Get(cfd, options, key, ctx);
        }
        catch
        {
            ctx.Callback = null;
            hooks.EndRead(cb, validate: false);
            throw;
        }
        ctx.Callback = null;
        hooks.EndRead(cb, validate: true);
    }

    /// <summary>Async <see cref="GetWithTxnHooks"/>; <paramref name="key"/> must stay valid until completion.</summary>
    private async ValueTask GetWithTxnHooksAsync(ColumnFamilyData cfd, ReadOptions? options, ReadOnlyMemory<byte> key, GetContext ctx,
        CancellationToken cancellationToken)
    {
        SeqPerBatchHooks hooks = _txnHooks!;
        ReadCallback cb = hooks.BeginRead(options?.Snapshot);
        ctx.Callback = cb;
        try
        {
            await GetWithTimestampAsync(cfd, options, key, ctx, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ctx.Callback = null;
            hooks.EndRead(cb, validate: false);
            throw;
        }
        ctx.Callback = null;
        hooks.EndRead(cb, validate: true);
    }

    /// <summary>
    /// DBImpl::GetImpl with an explicit read callback (WritePrepared / WriteUnprepared transactions): the resolved value
    /// is written to <paramref name="value"/>; returns false if the key does not exist.
    /// </summary>
    /// <summary>
    /// The DB read of MemTableInserter::PutCFImpl for an inplace_callback whose key is not in the memtable: a Get at
    /// <paramref name="sequence"/> (an unregistered snapshot) without filling the block cache. Returns false if absent.
    /// </summary>
    internal bool GetForInplaceCallback(ColumnFamilyData cfd, ReadOnlySpan<byte> key, ulong sequence, System.Buffers.IBufferWriter<byte> value)
    {
        var options = new ReadOptions { Snapshot = new Snapshot(this, sequence, 0), FillCache = false };
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        try
        {
            Get(cfd, options, key, ctx);
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> result)) return false;
            System.Buffers.BuffersExtensions.Write(value, result);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    internal bool GetWithCallback(ColumnFamilyData cfd, ReadOptions? options, ReadOnlySpan<byte> key, ReadCallback callback, System.Buffers.IBufferWriter<byte> value)
    {
        ThrowIfDisposed();
        GetContext ctx = GetContext.Rent(cfd.MergeOperator);
        try
        {
            ctx.Callback = callback;
            Get(cfd, options, key, ctx);
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> result)) return false;
            System.Buffers.BuffersExtensions.Write(value, result);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>An iterator reading through <paramref name="callback"/> up to <paramref name="sequence"/> (NewIteratorImpl with a read callback).</summary>
    internal Iterator NewIteratorWithCallback(ColumnFamilyData cfd, ReadOptions? options, ReadCallback callback, ulong sequence, Snapshot? ownedSnapshot)
    {
        ThrowIfDisposed();
        DbIteratorSources sources;
        try
        {
            sources = NewIteratorSources(cfd, options, callback, sequence, ownedSnapshot);
        }
        catch
        {
            ownedSnapshot?.Dispose();
            throw;
        }
        try
        {
            Iterator it = DbIteratorFactory.Create(sources);
            it.Statistics = Stats;
            return it;
        }
        catch
        {
            sources.Cleanup?.Dispose();
            throw;
        }
    }

    /// <summary>The lookup part of GetImpl on a referenced super version at <paramref name="snapshot"/>.</summary>
    [SkipLocalsInit]
    private void GetWithSuperVersion(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, ReadOnlySpan<byte> key, GetContext ctx)
    {
        // User-defined timestamps: the lookup key is key | read timestamp (validated here, after the SV reference).
        ReadOnlySpan<byte> ts = GetReadTimestamp(cfd, options);
        ctx.TimestampSize = cfd.TimestampSize;
        ctx.ReadSequence = snapshot;
        ctx.ReadFlags = TableReadFlagsExtensions.FromReadOptions(options);
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, snapshot, ts, scratch);
        try
        {
            long perfMem = PerfContext.StartTimer(); // PERF_TIMER_GUARD(get_from_memtable_time)
            bool fromMemTable = !SkipMemTables(options) && GetFromMemTables(sv, lkey, ctx);
            PerfContext.StopTimer(PerfMetric.GetFromMemtableTime, perfMem);
            if (fromMemTable)
            {
                ctx.FromMemTable = true;
                // A blob index is resolved while the super version (and so its blob files) is referenced.
                ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, key);
                return;
            }
            long perfFiles = PerfContext.StartTimer(); // PERF_TIMER_GUARD(get_from_output_files_time)
            GetFromVersion(cfd, sv.Current, lkey.InternalKey, lkey.UserKey, snapshot, ctx);
            ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, key);
            PerfContext.StopTimer(PerfMetric.GetFromOutputFilesTime, perfFiles);
        }
        finally
        {
            lkey.Dispose();
        }
    }

    /// <summary>
    /// GetImpl's skip_memtable: a read with read_tier = kPersistedTier skips the memtables once the DB holds data
    /// written without the WAL (has_unpersisted_data_), so it only returns data that survives a crash. (MemtableTier
    /// does not restrict point lookups upstream: only iterators skip the table files, NewInternalIterator.)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool SkipMemTables(ReadOptions? options) => options?.ReadTier == ReadTier.PersistedTier && Volatile.Read(ref _hasUnpersistedData);

    /// <summary>
    /// The read flags for a table file of <paramref name="level"/>: with optimize_filters_for_hits, filters are not
    /// consulted on the last non-empty level (Version::IsFilterSkipped: level &gt; 0 or the last L0 file, and the level
    /// is num_non_empty_levels - 1), since reaching it implies misses at all upper levels.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TableReadFlags WithFilterSkip(ColumnFamilyData cfd, VersionStorageInfo s, int level, bool isFileLastInLevel, TableReadFlags flags)
    {
        if (!cfd.Options.OptimizeFiltersForHits) return flags;
        return IsFilterSkipped(s, level, isFileLastInLevel) ? flags | TableReadFlags.SkipFilters : flags & ~TableReadFlags.SkipFilters;
    }

    /// <summary>Port of Version::IsFilterSkipped (version_set.cc) for a column family with optimize_filters_for_hits.</summary>
    internal static bool IsFilterSkipped(VersionStorageInfo s, int level, bool isFileLastInLevel) =>
        (level > 0 || isFileLastInLevel) && level == s.NumNonEmptyLevels - 1;

    /// <summary>
    /// FilePicker::GetNextFile: L0 files are skipped by key range unless the DB has a single non-empty level of at most
    /// three files ("the system is highly tuned to minimize number of tables queried by each query"); then every file
    /// is queried and its filter decides.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool CheckL0KeyRange(VersionStorageInfo s, int l0Files) => s.NumNonEmptyLevels > 1 || l0Files > 3;

    /// <summary>Memtable part of GetImpl. Returns true if a final result was found.</summary>
    private static bool GetFromMemTables(SuperVersion sv, scoped in LookupKey lkey, GetContext ctx)
    {
        GetContext? tsSink = ctx.TimestampSize != 0 ? ctx : null;
        ReadCallback? cb = ctx.Callback;
        int operands = ctx.MergeContext.Count;
        PerfContext.Add(PerfMetric.GetFromMemtableCount, 1); // MemTable::Get, once per memtable searched
        MemTableGetResult r = sv.Mem.Get(lkey, out ReadOnlySpan<byte> v, out EntryType t, out _, ctx.MergeContext, ref ctx.MaxCoveringTombstoneSeq, tsSink, cb);
        // GetMergeOperands with continue_cb: the callback may end the lookup at an operand of this memtable.
        if (ctx.ContinueCallback is not null && ctx.ApplyContinueCallback(operands)) return true;
        if (ctx.ApplyMemTableResult(r, v, t)) return true;
        MemTable[] imm = sv.Imm;
        for (int i = 0; i < imm.Length; i++)
        {
            operands = ctx.MergeContext.Count;
            PerfContext.Add(PerfMetric.GetFromMemtableCount, 1);
            r = imm[i].Get(lkey, out v, out t, out _, ctx.MergeContext, ref ctx.MaxCoveringTombstoneSeq, tsSink, cb);
            if (ctx.ContinueCallback is not null && ctx.ApplyContinueCallback(operands)) return true;
            if (ctx.ApplyMemTableResult(r, v, t)) return true;
        }
        return false;
    }

    /// <summary>
    /// Port of Version::Get: L0 files (all overlapping, newest first), then one file per sorted level found by binary
    /// search on the largest key.
    /// </summary>
    private static void GetFromVersion(ColumnFamilyData cfd, DbVersion v, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> userKey, ulong snapshot, GetContext ctx)
    {
        VersionStorageInfo s = v.Storage;
        Comparator ucmp = cfd.UserComparator;
        int tsSize = cfd.TimestampSize;
        bool sample = FileReadSample.ShouldSampleFileRead();
        ReadOnlySpan<FileMetaData> l0 = s.LevelFiles(0);
        bool checkL0Range = CheckL0KeyRange(s, l0.Length);
        for (int i = 0; i < l0.Length; i++)
        {
            FileMetaData f = l0[i];
            if (checkL0Range && (CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) < 0 || CompareFileBoundary(ucmp, tsSize, userKey, f.Largest) > 0)) continue;
            if (sample) FileReadSample.SampleFileReadInc(f);
            ctx.ReadFlags = WithFilterSkip(cfd, s, 0, i == l0.Length - 1, ctx.ReadFlags);
            bool done = GetFromFile(cfd, f, ikey, userKey, snapshot, ctx);
            if (sample) SampleCollapsibleRead(f, ctx);
            if (done)
            {
                ctx.HitLevel = 0;
                return;
            }
        }
        for (int level = 1; level < s.NumLevels; level++)
        {
            if (s.NumLevelFiles(level) == 0) continue;
            int idx = s.FindFile(level, ikey);
            ReadOnlySpan<FileMetaData> files = s.LevelFiles(level);
            if (idx >= files.Length) continue;
            FileMetaData f = files[idx];
            if (CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) < 0) continue;
            if (sample) FileReadSample.SampleFileReadInc(f);
            ctx.ReadFlags = WithFilterSkip(cfd, s, level, false, ctx.ReadFlags);
            bool done = GetFromFile(cfd, f, ikey, userKey, snapshot, ctx);
            if (sample) SampleCollapsibleRead(f, ctx);
            if (done)
            {
                ctx.HitLevel = level;
                return;
            }
        }
    }

    /// <summary>
    /// The sampling in Version::Get after a table lookup (version_set_sync_and_async.h): a lookup that left the key not
    /// found, collected merge operands or found a deletion is a collapsible-entry read of the file
    /// (sample_collapsible_entry_file_read_inc).
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    internal static void SampleCollapsibleRead(FileMetaData f, GetContext ctx)
    {
        if (ctx.State is GetContext.GetState.NotFound or GetContext.GetState.Merge or GetContext.GetState.Deleted)
        {
            FileReadSample.SampleCollapsibleEntryFileReadInc(f);
        }
    }

    /// <summary>
    /// User key vs. a file boundary (FilePicker): with user-defined timestamps the timestamps are ignored
    /// (CompareWithoutTimestamp), since newer versions of the boundary key sort before it.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareFileBoundary(Comparator ucmp, int tsSize, ReadOnlySpan<byte> userKey, byte[] boundary) =>
        tsSize == 0 ? ucmp.Compare(userKey, DbFormat.ExtractUserKey(boundary)) : ucmp.CompareWithoutTimestamp(userKey, DbFormat.ExtractUserKey(boundary));

    /// <summary>TableCache::Get: range tombstones of the file, then the point lookup. Returns true when done.</summary>
    private static bool GetFromFile(ColumnFamilyData cfd, FileMetaData f, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> userKey, ulong snapshot, GetContext ctx)
    {
        if (cfd.TableCache.RowCache is not null && UsesRowCache(ctx)) return GetFromFileWithRowCache(cfd, f, ikey, userKey, snapshot, ctx);
        return GetFromFileCore(cfd, f, ikey, userKey, snapshot, ctx);
    }

    /// <summary>
    /// Whether a lookup may use the row cache (TableCache::Get: not for lookups that need the sequence number or track
    /// newer versions). Deviation: lookups with a read callback (transactions), user-defined timestamps or a merge
    /// operand callback bypass it.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool UsesRowCache(GetContext ctx) => ctx.Callback is null && ctx.TimestampSize == 0 && ctx.ContinueCallback is null;

    /// <summary>
    /// TableCache::Get with a row cache: a hit replays the cached entries of the key in this file without opening the
    /// table (ROW_CACHE_HIT); a miss (ROW_CACHE_MISS) looks the key up and caches the entries it visited when there
    /// were any.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    [SkipLocalsInit]
    private static bool GetFromFileWithRowCache(ColumnFamilyData cfd, FileMetaData f, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> userKey, ulong snapshot,
        GetContext ctx)
    {
        TableCache tc = cfd.TableCache;
        Cache.RowCache rc = tc.RowCache!;
        int max = userKey.Length + TableCache.RowCacheKeyPrefixMax;
        byte[]? rented = null;
        Span<byte> buf = max <= 256 ? stackalloc byte[256] : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(max));
        try
        {
            int n = tc.RowCacheKey(buf, f, userKey, snapshot, out ulong replaySeq);
            ReadOnlySpan<byte> key = buf[..n];
            if (rc.TryGet(key, out byte[]? log))
            {
                tc.Statistics?.RecordTick(Ticker.RowCacheHit);
                ctx.Replay(log, userKey, replaySeq);
                return ctx.Done;
            }
            tc.Statistics?.RecordTick(Ticker.RowCacheMiss);
            PooledBufferWriter w = ObjectPools.RentWriter();
            try
            {
                bool done;
                ctx.ReplayLog = w;
                try
                {
                    done = GetFromFileCore(cfd, f, ikey, userKey, snapshot, ctx);
                }
                finally
                {
                    ctx.ReplayLog = null;
                }
                // Put the replay log in row cache only if something was found.
                if (w.WrittenCount > 0) rc.Insert(key, w.WrittenSpan);
                return done;
            }
            finally
            {
                ObjectPools.ReturnWriter(w);
            }
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Async <see cref="GetFromFileWithRowCache"/>: a miss reads the table with the async visitor lookup.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> GetFromFileWithRowCacheAsync(ColumnFamilyData cfd, FileMetaData f, ReadOnlyMemory<byte> ikey,
        ReadOnlyMemory<byte> userKey, ulong snapshot, GetContext ctx, CancellationToken cancellationToken)
    {
        TableCache tc = cfd.TableCache;
        Cache.RowCache rc = tc.RowCache!;
        byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(userKey.Length + TableCache.RowCacheKeyPrefixMax);
        PooledBufferWriter? w = null;
        try
        {
            int n = tc.RowCacheKey(buf, f, userKey.Span, snapshot, out ulong replaySeq);
            if (rc.TryGet(buf.AsSpan(0, n), out byte[]? log))
            {
                tc.Statistics?.RecordTick(Ticker.RowCacheHit);
                ctx.Replay(log, userKey.Span, replaySeq);
                return ctx.Done;
            }
            tc.Statistics?.RecordTick(Ticker.RowCacheMiss);
            w = ObjectPools.RentWriter();
            bool done;
            ctx.ReplayLog = w;
            try
            {
                done = await GetFromFileWithTimestampAsync(cfd, f, ikey, snapshot, ctx, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ctx.ReplayLog = null;
            }
            if (w.WrittenCount > 0) rc.Insert(buf.AsSpan(0, n), w.WrittenSpan);
            return done;
        }
        finally
        {
            if (w is not null) ObjectPools.ReturnWriter(w);
            System.Buffers.ArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <summary>The table lookup of <see cref="GetFromFile"/>.</summary>
    private static bool GetFromFileCore(ColumnFamilyData cfd, FileMetaData f, ReadOnlySpan<byte> ikey, ReadOnlySpan<byte> userKey, ulong snapshot, GetContext ctx)
    {
        TableReaderLease lease = cfd.TableCache.GetReaderForVersion(f, out FragmentedRangeTombstoneList? rangeDels, ctx.ReadFlags);
        try
        {
            if (rangeDels is not null)
            {
                // With user-defined timestamps the user key carries the read timestamp: it bounds the visible tombstones.
                int tsSize = ctx.TimestampSize;
                ulong cov = rangeDels.MaxCoveringTombstoneSeqnum(userKey, snapshot, tsSize == 0 ? default : userKey[^tsSize..], out ReadOnlySpan<byte> tombstoneTs);
                if (cov > ctx.MaxCoveringTombstoneSeq)
                {
                    ctx.MaxCoveringTombstoneSeq = cov;
                    if (tsSize != 0) ctx.SetTimestampFromRangeTombstone(tombstoneTs);
                }
            }
            // The sealed block-based reader is called directly (no generic virtual dispatch on the hot path).
            if (lease.Reader is BlockBasedTableReader bbt) bbt.Get(ikey, ref ctx, s_saveValue, ctx.ReadFlags);
            else lease.Reader.Get(ikey, ref ctx, s_saveValue, ctx.ReadFlags);
        }
        finally
        {
            lease.Dispose();
        }
        if (ctx.Done) return true;
        // A covering tombstone newer than anything older stops the search (no older entry can be visible).
        return false;
    }

    // ---- async ------------------------------------------------------------------------------------------

    /// <summary>
    /// Async point lookup: memtables are searched synchronously; table data blocks are read with async I/O (the first
    /// entry of the key via <see cref="BlockBasedTableReader.GetAsync"/>, falling back to the synchronous visitor for
    /// merge chains). <paramref name="key"/> must stay valid until completion.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask GetAsync(ColumnFamilyData cfd, ReadOptions? options, ReadOnlyMemory<byte> key, GetContext ctx, CancellationToken cancellationToken)
    {
        if (_queryTracer is { } tracer) TraceGet(tracer, cfd, key.Span, options);
        if (BlockCacheTracer.IsEnabled) SetBlockCacheTraceContext(Tracing.TableReaderCaller.UserGet, key.Span, options);
        if (_txnHooks is not null && ctx.Callback is null)
        {
            await GetWithTxnHooksAsync(cfd, options, key, ctx, cancellationToken).ConfigureAwait(false);
            return;
        }
        if (cfd.TimestampSize != 0 || ctx.Callback is not null)
        {
            // Timestamps and read callbacks visit every version of the key (the visitor path).
            await GetWithTimestampAsync(cfd, options, key, ctx, cancellationToken).ConfigureAwait(false);
            return;
        }
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetAndRefSuperVersion(cfd, out int svSlot);
        ulong snapshot = ReadSequence(options);
        ctx.ReadFlags = TableReadFlagsExtensions.FromReadOptions(options);
        PooledBufferWriter ikeyBuf = ObjectPools.RentWriter();
        try
        {
            if (!SkipMemTables(options) && GetFromMemTables(sv, key.Span, snapshot, ctx))
            {
                ctx.FromMemTable = true;
                await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, key, cancellationToken).ConfigureAwait(false);
                return;
            }
            Span<byte> dst = ikeyBuf.GetSpan(key.Length + DbFormat.InternalKeyFooterSize);
            int n = DbFormat.AppendInternalKey(dst, key.Span, snapshot, DbFormat.ValueTypeForSeek);
            ikeyBuf.Advance(n);
            ReadOnlyMemory<byte> ikey = ikeyBuf.WrittenMemory;

            VersionStorageInfo s = sv.Current.Storage;
            Comparator ucmp = cfd.UserComparator;
            bool sample = FileReadSample.ShouldSampleFileRead();
            FileMetaData[] l0 = s.LevelFilesArray(0);
            bool checkL0Range = CheckL0KeyRange(s, l0.Length);
            for (int i = 0; i < l0.Length; i++)
            {
                FileMetaData f = l0[i];
                if (checkL0Range && !UserKeyInFileRange(ucmp, key.Span, f)) continue;
                if (sample) FileReadSample.SampleFileReadInc(f);
                ctx.ReadFlags = WithFilterSkip(cfd, s, 0, i == l0.Length - 1, ctx.ReadFlags);
                bool done = await GetFromFileAsync(cfd, f, ikey, key, snapshot, ctx, cancellationToken).ConfigureAwait(false);
                if (sample) SampleCollapsibleRead(f, ctx);
                if (done)
                {
                    ctx.HitLevel = 0;
                    break;
                }
            }
            if (!ctx.Done)
            {
                for (int level = 1; level < s.NumLevels; level++)
                {
                    if (s.NumLevelFiles(level) == 0) continue;
                    int idx = s.FindFile(level, ikey.Span);
                    FileMetaData[] files = s.LevelFilesArray(level);
                    if (idx >= files.Length) continue;
                    FileMetaData f = files[idx];
                    if (ucmp.Compare(key.Span, DbFormat.ExtractUserKey(f.Smallest)) < 0) continue;
                    if (sample) FileReadSample.SampleFileReadInc(f);
                    ctx.ReadFlags = WithFilterSkip(cfd, s, level, false, ctx.ReadFlags);
                    bool done = await GetFromFileAsync(cfd, f, ikey, key, snapshot, ctx, cancellationToken).ConfigureAwait(false);
                    if (sample) SampleCollapsibleRead(f, ctx);
                    if (done)
                    {
                        ctx.HitLevel = level;
                        break;
                    }
                }
            }
            if (ctx.NeedsBlobResolution) await ctx.ResolveBlobAsync(cfd.BlobSource, s, options, key, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ObjectPools.ReturnWriter(ikeyBuf);
            ReturnAndCleanupSuperVersion(cfd, sv, svSlot);
        }
    }

    /// <summary>
    /// Async point lookup on a column family with user-defined timestamps: <see cref="GetWithSuperVersion"/> with the
    /// table lookups of <see cref="GetFromVersion"/> done by the async visitor Get (every version of the key is visited,
    /// like the synchronous path; data blocks are read with async I/O).
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask GetWithTimestampAsync(ColumnFamilyData cfd, ReadOptions? options, ReadOnlyMemory<byte> key, GetContext ctx,
        CancellationToken cancellationToken)
    {
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        ulong snapshot = ReadSequence(options);
        if (ctx.Callback is { } cb) snapshot = ApplyReadCallback(cb, options, snapshot);
        try
        {
            await GetWithSuperVersionAsync(cfd, sv, snapshot, options, key, ctx, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReturnSuperVersion(sv);
        }
    }

    /// <summary>
    /// Async <see cref="GetWithSuperVersion"/> for column families with user-defined timestamps (also used by the async
    /// timestamped MultiGet): memtables synchronously, table files with async data block reads.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask GetWithSuperVersionAsync(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, ReadOnlyMemory<byte> key,
        GetContext ctx, CancellationToken cancellationToken)
    {
        PooledBufferWriter ikeyBuf = ObjectPools.RentWriter();
        try
        {
            if (LookupMemTablesWithTimestamp(cfd, sv, snapshot, options, key.Span, ctx, ikeyBuf, SkipMemTables(options)))
            {
                ctx.FromMemTable = true;
                await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, key, cancellationToken).ConfigureAwait(false);
                return;
            }
            ReadOnlyMemory<byte> ikey = ikeyBuf.WrittenMemory;
            VersionStorageInfo s = sv.Current.Storage;
            Comparator ucmp = cfd.UserComparator;
            int tsSize = cfd.TimestampSize;
            FileMetaData[] l0 = s.LevelFilesArray(0);
            bool checkL0Range = CheckL0KeyRange(s, l0.Length);
            bool done = false;
            bool sample = FileReadSample.ShouldSampleFileRead();
            for (int i = 0; i < l0.Length && !done; i++)
            {
                FileMetaData f = l0[i];
                if (checkL0Range && !UserKeyInFileRangeWithoutTimestamp(ucmp, tsSize, ikey, f)) continue;
                if (sample) FileReadSample.SampleFileReadInc(f);
                ctx.ReadFlags = WithFilterSkip(cfd, s, 0, i == l0.Length - 1, ctx.ReadFlags);
                done = await GetFromFileWithTimestampAsync(cfd, f, ikey, snapshot, ctx, cancellationToken).ConfigureAwait(false);
                if (sample) SampleCollapsibleRead(f, ctx);
                if (done) ctx.HitLevel = 0;
            }
            for (int level = 1; level < s.NumLevels && !done; level++)
            {
                if (s.NumLevelFiles(level) == 0) continue;
                int idx = s.FindFile(level, ikey.Span);
                FileMetaData[] files = s.LevelFilesArray(level);
                if (idx >= files.Length) continue;
                FileMetaData f = files[idx];
                if (CompareFileBoundary(ucmp, tsSize, DbFormat.ExtractUserKey(ikey.Span), f.Smallest) < 0) continue;
                if (sample) FileReadSample.SampleFileReadInc(f);
                ctx.ReadFlags = WithFilterSkip(cfd, s, level, false, ctx.ReadFlags);
                done = await GetFromFileWithTimestampAsync(cfd, f, ikey, snapshot, ctx, cancellationToken).ConfigureAwait(false);
                if (sample) SampleCollapsibleRead(f, ctx);
                if (done) ctx.HitLevel = level;
            }
            if (ctx.NeedsBlobResolution) await ctx.ResolveBlobAsync(cfd.BlobSource, s, options, key, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ObjectPools.ReturnWriter(ikeyBuf);
        }
    }

    /// <summary>
    /// Synchronous memtable part of <see cref="GetWithTimestampAsync"/>; leaves the internal lookup key
    /// (key | read ts | snapshot, kValueTypeForSeek) in <paramref name="ikeyBuf"/>. Returns true if a final result was found.
    /// </summary>
    [SkipLocalsInit]
    private static bool LookupMemTablesWithTimestamp(ColumnFamilyData cfd, SuperVersion sv, ulong snapshot, ReadOptions? options, ReadOnlySpan<byte> key,
        GetContext ctx, PooledBufferWriter ikeyBuf, bool skipMemTables)
    {
        ReadOnlySpan<byte> ts = GetReadTimestamp(cfd, options);
        ctx.TimestampSize = cfd.TimestampSize;
        ctx.ReadSequence = snapshot;
        ctx.ReadFlags = TableReadFlagsExtensions.FromReadOptions(options);
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, snapshot, ts, scratch);
        try
        {
            ikeyBuf.Write(lkey.InternalKey);
            return !skipMemTables && GetFromMemTables(sv, lkey, ctx);
        }
        finally
        {
            lkey.Dispose();
        }
    }

    private static bool UserKeyInFileRangeWithoutTimestamp(Comparator ucmp, int tsSize, ReadOnlyMemory<byte> ikey, FileMetaData f)
    {
        ReadOnlySpan<byte> userKey = DbFormat.ExtractUserKey(ikey.Span);
        return CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) >= 0 && CompareFileBoundary(ucmp, tsSize, userKey, f.Largest) <= 0;
    }

    /// <summary>Async <see cref="GetFromFile"/> (TableCache::Get with the read timestamp bounding the range tombstones).</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> GetFromFileWithTimestampAsync(ColumnFamilyData cfd, FileMetaData f, ReadOnlyMemory<byte> ikey, ulong snapshot,
        GetContext ctx, CancellationToken cancellationToken)
    {
        // Opening a table is synchronous (TableCache.GetReaderAsync); the super version keeps the file's version referenced.
        TableReaderLease lease = cfd.TableCache.GetReaderForVersion(f, out FragmentedRangeTombstoneList? rangeDels, ctx.ReadFlags);
        try
        {
            ApplyFileRangeTombstones(rangeDels, DbFormat.ExtractUserKey(ikey.Span), snapshot, ctx);
            ValueTask pendingGet = lease.Reader is BlockBasedTableReader bbt
                ? bbt.GetAsync(ikey, ctx, s_saveValue, ctx.ReadFlags, cancellationToken)
                : lease.Reader.GetAsync(ikey, ctx, s_saveValue, ctx.ReadFlags, cancellationToken);
            await pendingGet.ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
        return ctx.Done;
    }

    private static void ApplyFileRangeTombstones(FragmentedRangeTombstoneList? rangeDels, ReadOnlySpan<byte> userKey, ulong snapshot, GetContext ctx)
    {
        if (rangeDels is null) return;
        int tsSize = ctx.TimestampSize;
        ulong cov = rangeDels.MaxCoveringTombstoneSeqnum(userKey, snapshot, tsSize == 0 ? default : userKey[^tsSize..], out ReadOnlySpan<byte> tombstoneTs);
        if (cov > ctx.MaxCoveringTombstoneSeq)
        {
            ctx.MaxCoveringTombstoneSeq = cov;
            if (tsSize != 0) ctx.SetTimestampFromRangeTombstone(tombstoneTs);
        }
    }

    private static bool UserKeyInFileRange(Comparator ucmp, ReadOnlySpan<byte> userKey, FileMetaData f) =>
        ucmp.Compare(userKey, DbFormat.ExtractUserKey(f.Smallest)) >= 0 && ucmp.Compare(userKey, DbFormat.ExtractUserKey(f.Largest)) <= 0;

    [SkipLocalsInit]
    private static bool GetFromMemTables(SuperVersion sv, ReadOnlySpan<byte> key, ulong snapshot, GetContext ctx)
    {
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, snapshot, scratch);
        try
        {
            return GetFromMemTables(sv, lkey, ctx);
        }
        finally
        {
            lkey.Dispose();
        }
    }

    /// <summary>
    /// Async <see cref="GetFromFile"/>: the first entry of the key is read with <see cref="BlockBasedTableReader.GetAsync"/>
    /// (merge chains continue with the synchronous visitor). Completes without an async state machine when the table
    /// lookup does (filter miss, cached blocks).
    /// </summary>
    private static ValueTask<bool> GetFromFileAsync(ColumnFamilyData cfd, FileMetaData f, ReadOnlyMemory<byte> ikey, ReadOnlyMemory<byte> userKey,
        ulong snapshot, GetContext ctx, CancellationToken cancellationToken)
    {
        if (cfd.TableCache.RowCache is not null && UsesRowCache(ctx)) return GetFromFileWithRowCacheAsync(cfd, f, ikey, userKey, snapshot, ctx, cancellationToken);
        // Opening a table is synchronous (TableCache.GetReaderAsync); the super version keeps the file's version referenced.
        TableReaderLease lease = cfd.TableCache.GetReaderForVersion(f, out FragmentedRangeTombstoneList? rangeDels, ctx.ReadFlags);
        ValueTask<TableGetResult> pending;
        try
        {
            if (rangeDels is not null)
            {
                ulong cov = rangeDels.MaxCoveringTombstoneSeqnum(userKey.Span, snapshot);
                if (cov > ctx.MaxCoveringTombstoneSeq) ctx.MaxCoveringTombstoneSeq = cov;
            }
            ctx.Scratch.Clear();
            pending = lease.Reader is BlockBasedTableReader bbt
                ? bbt.GetAsync(ikey, ctx.Scratch, ctx.ReadFlags, cancellationToken)
                : lease.Reader.GetAsync(ikey, ctx.Scratch, ctx.ReadFlags, cancellationToken);
            if (pending.IsCompletedSuccessfully)
            {
                SaveFileResult(lease.Reader, pending.Result, ikey.Span, ctx);
                lease.Dispose();
                return new ValueTask<bool>(ctx.Done);
            }
        }
        catch
        {
            lease.Dispose();
            throw;
        }
        return GetFromFileSlowAsync(lease, pending, ikey, ctx);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> GetFromFileSlowAsync(TableReaderLease lease, ValueTask<TableGetResult> pending, ReadOnlyMemory<byte> ikey, GetContext ctx)
    {
        try
        {
            TableGetResult r = await pending.ConfigureAwait(false);
            SaveFileResult(lease.Reader, r, ikey.Span, ctx);
        }
        finally
        {
            lease.Dispose();
        }
        return ctx.Done;
    }

    private static void SaveFileResult(TableReader reader, TableGetResult r, ReadOnlySpan<byte> ikey, GetContext ctx)
    {
        if (!r.Found) return;
        if (r.Type == EntryType.Merge && ctx.MaxCoveringTombstoneSeq <= r.Sequence)
        {
            // Merge chain: the rest of the operands are most likely in the same (now cached by the OS) block.
            if (reader is BlockBasedTableReader bbt) bbt.Get(ikey, ref ctx, s_saveValue, ctx.ReadFlags);
            else reader.Get(ikey, ref ctx, s_saveValue, ctx.ReadFlags);
        }
        else
        {
            ctx.SaveEntry(r.Sequence, r.Type, ctx.Scratch.WrittenSpan);
        }
    }

    // ---- iterators --------------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::NewIterator: a DBIter over the memtables and table files of a referenced super version, read at
    /// the snapshot (or latest) sequence. The super version reference is released when the iterator is disposed.
    /// </summary>
    public DbIteratorSources NewIteratorSources(ColumnFamilyData cfd, ReadOptions? options)
    {
        if (_txnHooks is { } hooks)
        {
            // WritePreparedTxnDB::NewIterator: the iterator reads at a snapshot (its own when none is given, released
            // with the iterator) through a read callback backed by that snapshot.
            Snapshot? own = options?.Snapshot is null ? hooks.GetSnapshot(isWriteConflictBoundary: false) : null;
            Snapshot snap = options?.Snapshot ?? own!;
            try
            {
                return NewIteratorSources(cfd, options, hooks.NewIteratorCallback(snap), snap.SequenceNumber, own);
            }
            catch
            {
                own?.Dispose();
                throw;
            }
        }
        return NewIteratorSources(cfd, options, null, DbFormat.MaxSequenceNumber, null);
    }

    /// <summary>
    /// DBImpl::NewIteratorImpl with a read callback: <paramref name="sequence"/> is the largest visible sequence
    /// (<see cref="DbFormat.MaxSequenceNumber"/> = the snapshot's or the latest); <paramref name="ownedSnapshot"/> is
    /// released with the iterator.
    /// </summary>
    /// <summary>A copy of <paramref name="options"/> (or default read options) with total_order_seek set.</summary>
    private static ReadOptions WithTotalOrderSeek(ReadOptions? options)
    {
        ReadOptions o = options?.Clone() ?? new ReadOptions();
        o.TotalOrderSeek = true;
        return o;
    }

    internal DbIteratorSources NewIteratorSources(ColumnFamilyData cfd, ReadOptions? options, ReadCallback? callback, ulong sequence, Snapshot? ownedSnapshot)
    {
        // DBImpl::NewIterator (db_impl.cc): kPersistedTier is rejected for iterators.
        if (options?.ReadTier == ReadTier.PersistedTier)
        {
            throw new RocksDbException(StatusCode.NotSupported, "Not implemented: ReadTier::kPersistedData is not yet supported in iterators.");
        }
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        if (sequence == DbFormat.MaxSequenceNumber)
        {
            sequence = ReadSequence(options);
            if (callback is not null) sequence = ApplyReadCallback(callback, options, sequence);
            // A tailing iterator (ForwardIterator under DBIter at kMaxSequenceNumber) sees every write, also those made
            // after it was created; the snapshot does not apply.
            if (options?.Tailing == true && callback is null) sequence = DbFormat.MaxSequenceNumber;
        }
        try
        {
            GetReadTimestamp(cfd, options);
            if (options?.IterStartTs is not null && options.Timestamp is null) ThrowHelper.InvalidArgument("iter_start_ts requires timestamp");
        }
        catch
        {
            ReturnSuperVersion(sv);
            throw;
        }
        // ArenaWrappedDBIter::Init: read_options_.total_order_seek |= prefix_seek_opt_in_only.
        if (Options.PrefixSeekOptInOnly && options?.TotalOrderSeek != true) options = WithTotalOrderSeek(options);
        var mems = new MemTable[1 + sv.Imm.Length];
        mems[0] = sv.Mem;
        sv.Imm.CopyTo(mems, 1);
        VersionStorageInfo s = sv.Current.Storage;
        var levels = new IReadOnlyList<FileMetaData>[s.NumLevels];
        // NewInternalIterator: with kMemtableTier only the memtables are iterated (no table file, no file tombstone).
        bool memtableOnly = options?.ReadTier == ReadTier.MemtableTier;
        for (int l = 0; l < levels.Length; l++) levels[l] = memtableOnly ? [] : s.LevelFilesArray(l);
        return new DbIteratorSources
        {
            Comparator = cfd.Icmp,
            ReadOptions = options,
            Sequence = sequence,
            MergeOperator = cfd.MergeOperator,
            MemTables = mems,
            Levels = levels,
            TableCache = cfd.TableCache,
            MaxSequentialSkipInIterations = sv.MutableOptions.MaxSequentialSkipInIterations,
            BlobSource = cfd.BlobSource,
            BlobVersion = s,
            PrefixExtractor = sv.MutableOptions.PrefixExtractor,
            OptimizeFiltersForHits = cfd.Options.OptimizeFiltersForHits,
            NumNonEmptyLevels = s.NumNonEmptyLevels,
            ReadCallback = callback,
            Cleanup = new SuperVersionRelease(this, sv, ownedSnapshot),
        };
    }

    /// <summary>
    /// A tailing iterator (ReadOptions::tailing, port of the ForwardIterator behaviour of DBImpl::NewIterator): reads
    /// at kMaxSequenceNumber, so writes made after its creation become visible to later Seek / Next calls, and its
    /// memtable / table iterators are rebuilt from the current super version when the column family installed a new
    /// one (ForwardIterator::RenewIterators on Seek, SeekToFirst and Next). SeekToLast, SeekForPrev and Prev are
    /// NotSupported like upstream. Deviation: the stack is the regular DBIter stack rebuilt on renewal, not
    /// ForwardIterator's incremental per-level iterators.
    /// </summary>
    internal Iterator NewTailingIterator(ColumnFamilyData cfd, ReadOptions options)
    {
        var source = new TailingIteratorSource(this, cfd, options);
        var it = new Iterator(source.Build());
        it.SetTailing(source);
        return it;
    }

    private sealed class TailingIteratorSource(DbImpl db, ColumnFamilyData cfd, ReadOptions options) : Iterator.TailingSource
    {
        private ulong _number;

        public override bool IsStale => cfd.Super.Number != _number;

        public override DbIter Build()
        {
            // Read before the super version is referenced: a change in between only causes one extra renewal.
            ulong number = cfd.Super.Number;
            DbIteratorSources sources = db.NewIteratorSources(cfd, options);
            try
            {
                DbIter it = DbIteratorFactory.CreateDbIter(sources);
                _number = number;
                return it;
            }
            catch
            {
                sources.Cleanup?.Dispose();
                throw;
            }
        }
    }

    /// <summary>
    /// Releases an iterator's super version reference (CleanupSuperVersionHandle) and the snapshot it owns
    /// (WritePreparedTxnDB::IteratorState).
    /// </summary>
    private sealed class SuperVersionRelease(DbImpl db, SuperVersion sv, Snapshot? ownedSnapshot) : IDisposable
    {
        private int _done;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _done, 1) == 0)
            {
                db.ReturnSuperVersion(sv);
                ownedSnapshot?.Dispose();
            }
        }
    }
}
