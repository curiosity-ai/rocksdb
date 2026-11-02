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
        if (snap is null) return Versions.LastSequence;
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
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        TestHooks.Invoke("DBImpl::GetImpl:AfterSuperVersion");
        ulong snapshot = ReadSequence(options);
        try
        {
            GetWithSuperVersion(cfd, sv, snapshot, options, key, ctx);
        }
        finally
        {
            ReturnSuperVersion(sv);
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
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, snapshot, ts, scratch);
        try
        {
            if (GetFromMemTables(sv, lkey, ctx))
            {
                ctx.FromMemTable = true;
                // A blob index is resolved while the super version (and so its blob files) is referenced.
                ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, key);
                return;
            }
            if (options?.ReadTier == ReadTier.MemtableTier)
            {
                if (ctx.State == GetContext.GetState.NotFound) return;
                ThrowHelper.Corruption("Incomplete: merge operands not all in memtables");
            }
            GetFromVersion(cfd, sv.Current, lkey.InternalKey, lkey.UserKey, snapshot, ctx);
            ctx.ResolveBlob(cfd.BlobSource, sv.Current.Storage, options, key);
        }
        finally
        {
            lkey.Dispose();
        }
    }

    /// <summary>Memtable part of GetImpl. Returns true if a final result was found.</summary>
    private static bool GetFromMemTables(SuperVersion sv, scoped in LookupKey lkey, GetContext ctx)
    {
        GetContext? tsSink = ctx.TimestampSize != 0 ? ctx : null;
        MemTableGetResult r = sv.Mem.Get(lkey, out ReadOnlySpan<byte> v, out EntryType t, out _, ctx.MergeContext, ref ctx.MaxCoveringTombstoneSeq, tsSink);
        if (ctx.ApplyMemTableResult(r, v, t)) return true;
        MemTable[] imm = sv.Imm;
        for (int i = 0; i < imm.Length; i++)
        {
            r = imm[i].Get(lkey, out v, out t, out _, ctx.MergeContext, ref ctx.MaxCoveringTombstoneSeq, tsSink);
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
        ReadOnlySpan<FileMetaData> l0 = s.LevelFiles(0);
        for (int i = 0; i < l0.Length; i++)
        {
            FileMetaData f = l0[i];
            if (CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) < 0 || CompareFileBoundary(ucmp, tsSize, userKey, f.Largest) > 0) continue;
            if (GetFromFile(cfd, f, ikey, userKey, snapshot, ctx)) return;
        }
        for (int level = 1; level < s.NumLevels; level++)
        {
            if (s.NumLevelFiles(level) == 0) continue;
            int idx = s.FindFile(level, ikey);
            ReadOnlySpan<FileMetaData> files = s.LevelFiles(level);
            if (idx >= files.Length) continue;
            FileMetaData f = files[idx];
            if (CompareFileBoundary(ucmp, tsSize, userKey, f.Smallest) < 0) continue;
            if (GetFromFile(cfd, f, ikey, userKey, snapshot, ctx)) return;
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
        TableReaderLease lease = cfd.TableCache.GetReader(f, out FragmentedRangeTombstoneList? rangeDels);
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
            lease.Reader.Get(ikey, ref ctx, s_saveValue);
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
        if (cfd.TimestampSize != 0)
        {
            await GetWithTimestampAsync(cfd, options, key, ctx, cancellationToken).ConfigureAwait(false);
            return;
        }
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        ulong snapshot = ReadSequence(options);
        PooledBufferWriter ikeyBuf = ObjectPools.RentWriter();
        try
        {
            if (GetFromMemTables(sv, key.Span, snapshot, ctx))
            {
                ctx.FromMemTable = true;
                await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, key, cancellationToken).ConfigureAwait(false);
                return;
            }
            if (options?.ReadTier == ReadTier.MemtableTier)
            {
                if (ctx.State == GetContext.GetState.NotFound) return;
                ThrowHelper.Corruption("Incomplete: merge operands not all in memtables");
            }
            Span<byte> dst = ikeyBuf.GetSpan(key.Length + DbFormat.InternalKeyFooterSize);
            int n = DbFormat.AppendInternalKey(dst, key.Span, snapshot, DbFormat.ValueTypeForSeek);
            ikeyBuf.Advance(n);
            ReadOnlyMemory<byte> ikey = ikeyBuf.WrittenMemory;

            VersionStorageInfo s = sv.Current.Storage;
            Comparator ucmp = cfd.UserComparator;
            FileMetaData[] l0 = s.LevelFilesArray(0);
            for (int i = 0; i < l0.Length; i++)
            {
                FileMetaData f = l0[i];
                if (!UserKeyInFileRange(ucmp, key.Span, f)) continue;
                if (await GetFromFileAsync(cfd, f, ikey, key, snapshot, ctx, cancellationToken).ConfigureAwait(false)) break;
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
                    if (await GetFromFileAsync(cfd, f, ikey, key, snapshot, ctx, cancellationToken).ConfigureAwait(false)) break;
                }
            }
            if (ctx.ValueIsBlobIndex) await ctx.ResolveBlobAsync(cfd.BlobSource, s, options, key, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ObjectPools.ReturnWriter(ikeyBuf);
            ReturnSuperVersion(sv);
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
            if (LookupMemTablesWithTimestamp(cfd, sv, snapshot, options, key.Span, ctx, ikeyBuf))
            {
                ctx.FromMemTable = true;
                await ctx.ResolveBlobAsync(cfd.BlobSource, sv.Current.Storage, options, key, cancellationToken).ConfigureAwait(false);
                return;
            }
            if (options?.ReadTier == ReadTier.MemtableTier)
            {
                if (ctx.State == GetContext.GetState.NotFound) return;
                ThrowHelper.Corruption("Incomplete: merge operands not all in memtables");
            }
            ReadOnlyMemory<byte> ikey = ikeyBuf.WrittenMemory;
            VersionStorageInfo s = sv.Current.Storage;
            Comparator ucmp = cfd.UserComparator;
            int tsSize = cfd.TimestampSize;
            FileMetaData[] l0 = s.LevelFilesArray(0);
            bool done = false;
            for (int i = 0; i < l0.Length && !done; i++)
            {
                FileMetaData f = l0[i];
                if (!UserKeyInFileRangeWithoutTimestamp(ucmp, tsSize, ikey, f)) continue;
                done = await GetFromFileWithTimestampAsync(cfd, f, ikey, snapshot, ctx, cancellationToken).ConfigureAwait(false);
            }
            for (int level = 1; level < s.NumLevels && !done; level++)
            {
                if (s.NumLevelFiles(level) == 0) continue;
                int idx = s.FindFile(level, ikey.Span);
                FileMetaData[] files = s.LevelFilesArray(level);
                if (idx >= files.Length) continue;
                FileMetaData f = files[idx];
                if (CompareFileBoundary(ucmp, tsSize, DbFormat.ExtractUserKey(ikey.Span), f.Smallest) < 0) continue;
                done = await GetFromFileWithTimestampAsync(cfd, f, ikey, snapshot, ctx, cancellationToken).ConfigureAwait(false);
            }
            if (ctx.ValueIsBlobIndex) await ctx.ResolveBlobAsync(cfd.BlobSource, s, options, key, cancellationToken).ConfigureAwait(false);
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
        GetContext ctx, PooledBufferWriter ikeyBuf)
    {
        ReadOnlySpan<byte> ts = GetReadTimestamp(cfd, options);
        ctx.TimestampSize = cfd.TimestampSize;
        ctx.ReadSequence = snapshot;
        Span<byte> scratch = stackalloc byte[LookupKey.StackBufferSize];
        var lkey = new LookupKey(key, snapshot, ts, scratch);
        try
        {
            ikeyBuf.Write(lkey.InternalKey);
            return GetFromMemTables(sv, lkey, ctx);
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
        TableReaderLease lease = await cfd.TableCache.GetReaderAsync(f, cancellationToken).ConfigureAwait(false);
        try
        {
            ApplyFileRangeTombstones(cfd.TableCache.GetRangeTombstones(f), DbFormat.ExtractUserKey(ikey.Span), snapshot, ctx);
            await lease.Reader.GetAsync(ikey, ctx, s_saveValue, TableReadFlags.None, cancellationToken).ConfigureAwait(false);
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<bool> GetFromFileAsync(ColumnFamilyData cfd, FileMetaData f, ReadOnlyMemory<byte> ikey, ReadOnlyMemory<byte> userKey,
        ulong snapshot, GetContext ctx, CancellationToken cancellationToken)
    {
        TableReaderLease lease = await cfd.TableCache.GetReaderAsync(f, cancellationToken).ConfigureAwait(false);
        try
        {
            FragmentedRangeTombstoneList? rangeDels = cfd.TableCache.GetRangeTombstones(f);
            if (rangeDels is not null)
            {
                ulong cov = rangeDels.MaxCoveringTombstoneSeqnum(userKey.Span, snapshot);
                if (cov > ctx.MaxCoveringTombstoneSeq) ctx.MaxCoveringTombstoneSeq = cov;
            }
            ctx.Scratch.Clear();
            TableGetResult r = await lease.Reader.GetAsync(ikey, ctx.Scratch, cancellationToken).ConfigureAwait(false);
            if (r.Found)
            {
                if (r.Type == EntryType.Merge && ctx.MaxCoveringTombstoneSeq <= r.Sequence)
                {
                    // Merge chain: the rest of the operands are most likely in the same (now cached by the OS) block.
                    lease.Reader.Get(ikey.Span, ref ctx, s_saveValue);
                }
                else
                {
                    ctx.SaveEntry(r.Sequence, r.Type, ctx.Scratch.WrittenSpan);
                }
            }
        }
        finally
        {
            lease.Dispose();
        }
        return ctx.Done;
    }

    // ---- iterators --------------------------------------------------------------------------------------

    /// <summary>
    /// Port of DBImpl::NewIterator: a DBIter over the memtables and table files of a referenced super version, read at
    /// the snapshot (or latest) sequence. The super version reference is released when the iterator is disposed.
    /// </summary>
    public DbIteratorSources NewIteratorSources(ColumnFamilyData cfd, ReadOptions? options)
    {
        // The super version is referenced BEFORE the read sequence is taken (GetImpl): otherwise a flush + compaction
        // in between could drop the versions visible at that sequence (no snapshot protects them).
        ValidateReadSnapshot(options);
        SuperVersion sv = GetReferencedSuperVersion(cfd);
        ulong sequence = ReadSequence(options);
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
        var mems = new MemTable[1 + sv.Imm.Length];
        mems[0] = sv.Mem;
        sv.Imm.CopyTo(mems, 1);
        VersionStorageInfo s = sv.Current.Storage;
        var levels = new IReadOnlyList<FileMetaData>[s.NumLevels];
        for (int l = 0; l < levels.Length; l++) levels[l] = s.LevelFilesArray(l);
        return new DbIteratorSources
        {
            Comparator = cfd.Icmp,
            ReadOptions = options,
            Sequence = sequence,
            MergeOperator = cfd.MergeOperator,
            MemTables = mems,
            Levels = levels,
            TableCache = cfd.TableCache,
            MaxSequentialSkipInIterations = cfd.Options.MaxSequentialSkipInIterations,
            BlobSource = cfd.BlobSource,
            BlobVersion = s,
            PrefixExtractor = cfd.Options.PrefixExtractor,
            Cleanup = new SuperVersionRelease(this, sv),
        };
    }

    /// <summary>Releases an iterator's super version reference (CleanupSuperVersionHandle).</summary>
    private sealed class SuperVersionRelease(DbImpl db, SuperVersion sv) : IDisposable
    {
        private int _done;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _done, 1) == 0) db.ReturnSuperVersion(sv);
        }
    }
}
