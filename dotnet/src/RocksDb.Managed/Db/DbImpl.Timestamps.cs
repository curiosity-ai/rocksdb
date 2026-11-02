using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Db;

/// <summary>
/// User-defined timestamp support of the DB layer: read-timestamp validation (DBImpl::FailIfTsMismatchCf,
/// FailIfCfHasTs, FailIfReadCollapsedHistory in db/db_impl/db_impl.h), full_history_ts_low
/// (DBImpl::IncreaseFullHistoryTsLow / IncreaseFullHistoryTsLowImpl in db/db_impl/db_impl_compaction_flush.cc) and the
/// column family timestamp sizes recorded in the WAL (VersionSet::GetColumnFamiliesTimestampSizeForRecord).
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>
    /// Timestamp sizes of the live column families with timestamps (cf id -&gt; size), or null when there are none.
    /// Copy-on-write, replaced under the DB mutex; read by the write leader before each WAL write.
    /// </summary>
    private volatile KeyValuePair<uint, int>[]? _cfTimestampSizes;

    /// <summary>Recomputes <see cref="_cfTimestampSizes"/> from the write map (called when it changes).</summary>
    private void RefreshTimestampSizes(ColumnFamilyData?[] map)
    {
        List<KeyValuePair<uint, int>>? list = null;
        foreach (ColumnFamilyData? c in map)
        {
            if (c is null || c.Dropped || c.TimestampSize == 0) continue;
            (list ??= new List<KeyValuePair<uint, int>>()).Add(new KeyValuePair<uint, int>(c.Id, c.TimestampSize));
        }
        _cfTimestampSizes = list?.ToArray();
    }

    /// <summary>Timestamp size of a live column family by id (0 if unknown or without timestamps).</summary>
    internal int TimestampSizeOf(uint columnFamilyId)
    {
        KeyValuePair<uint, int>[]? sizes = _cfTimestampSizes;
        if (sizes is null) return 0;
        foreach (KeyValuePair<uint, int> kv in sizes)
        {
            if (kv.Key == columnFamilyId) return kv.Value;
        }
        return 0;
    }

    /// <summary>
    /// Validates <see cref="ReadOptions.Timestamp"/> / <see cref="ReadOptions.IterStartTs"/> against the column family
    /// (GetImpl / NewIterator checks) and returns the read timestamp (empty for column families without timestamps).
    /// Call after the super version is referenced.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ReadOnlySpan<byte> GetReadTimestamp(ColumnFamilyData cfd, ReadOptions? options)
    {
        if (cfd.TimestampSize == 0)
        {
            if (options is not null && (options.Timestamp is not null || options.IterStartTs is not null)) ThrowTimestampOnNonTimestampCf(cfd);
            return default;
        }
        return GetReadTimestampSlow(cfd, options);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowTimestampOnNonTimestampCf(ColumnFamilyData cfd) =>
        ThrowHelper.InvalidArgument("cannot call this method on column family " + cfd.Name + " that does not enable timestamp");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ReadOnlySpan<byte> GetReadTimestampSlow(ColumnFamilyData cfd, ReadOptions? options)
    {
        if (options?.Timestamp is not ReadOnlyMemory<byte> tsMem)
        {
            ThrowHelper.InvalidArgument("cannot call this method on column family " + cfd.Name + " that enables timestamp");
            return default;
        }
        ReadOnlySpan<byte> ts = tsMem.Span;
        ColumnFamily.FailIfTsMismatchCf(cfd, ts.Length);
        if (options.IterStartTs is ReadOnlyMemory<byte> start) ColumnFamily.FailIfTsMismatchCf(cfd, start.Length);
        FailIfReadCollapsedHistory(cfd, ts);
        return ts;
    }

    /// <summary>
    /// Port of DBImpl::FailIfReadCollapsedHistory. Deviation: checks the column family's current full_history_ts_low
    /// (which only increases) instead of the super version's copy, so it may reject a read upstream would still allow,
    /// never the reverse.
    /// </summary>
    private static void FailIfReadCollapsedHistory(ColumnFamilyData cfd, ReadOnlySpan<byte> ts)
    {
        byte[]? low = cfd.FullHistoryTsLow;
        Comparator ucmp = cfd.UserComparator;
        if (low is not null && ucmp.CompareTimestamp(ts, low) < 0)
        {
            ThrowHelper.InvalidArgument("Read timestamp: " + ucmp.TimestampToString(ts) + " is smaller than full_history_ts_low: " +
                                        ucmp.TimestampToString(low) + "\n");
        }
    }

    /// <summary>Port of DBImpl::GetNewestUserDefinedTimestamp.</summary>
    internal byte[] GetNewestUserDefinedTimestamp(ColumnFamilyData cfd)
    {
        if (cfd.TimestampSize == 0) ThrowHelper.InvalidArgument("Timestamp is not enabled in this column family");
        if (!cfd.TimestampsInMemtableOnly)
        {
            ThrowHelper.NotSupported("GetNewestUserDefinedTimestamp doesn't support the case when userdefined timestamps are persisted.");
        }
        SuperVersion sv;
        lock (_mutex) sv = cfd.Super;
        // The newest timestamp is tracked under the memtable's own lock, so no write-thread entry is needed.
        byte[]? newest = sv.Mem.GetNewestUdt();
        if (newest is null)
        {
            foreach (MemTable m in sv.Imm)
            {
                newest = m.GetNewestUdt();
                if (newest is not null) break;
            }
        }
        if (newest is null && sv.Current.Storage.NumFiles > 0 && cfd.FullHistoryTsLow is { Length: sizeof(ulong) } low)
        {
            // GetU64CutoffTsFromFullHistoryTsLow: full_history_ts_low is the exclusive upper bound of flushed timestamps.
            ulong v = Util.Coding.DecodeFixed64(low);
            newest = new byte[sizeof(ulong)];
            Util.Coding.EncodeFixed64(newest, v > 0 ? v - 1 : 0);
        }
        return newest ?? [];
    }

    /// <summary>Port of DBImpl::IncreaseFullHistoryTsLow + IncreaseFullHistoryTsLowImpl.</summary>
    internal async ValueTask IncreaseFullHistoryTsLowAsync(ColumnFamilyData cfd, byte[] tsLow, bool synchronous, CancellationToken ct)
    {
        ThrowIfDisposed();
        if (_readOnly) ThrowReadOnly();
        Comparator ucmp = cfd.UserComparator;
        if (cfd.TimestampSize == 0) ThrowHelper.InvalidArgument("Timestamp is not enabled in this column family");
        if (cfd.TimestampSize != tsLow.Length) ThrowHelper.InvalidArgument("ts_low size mismatch");
        byte[]? current = cfd.FullHistoryTsLow;
        if (current is not null && ucmp.CompareTimestamp(tsLow, current) < 0)
        {
            ThrowHelper.InvalidArgument("Current full_history_ts_low: " + ucmp.TimestampToString(current) + " is higher than provided ts: " +
                                        ucmp.TimestampToString(tsLow) + "\n");
        }
        var edit = new VersionEdit { ColumnFamily = cfd.Id, FullHistoryTsLow = tsLow };
        await Versions.LogAndApplyAsync(cfd, edit, synchronous, ct).ConfigureAwait(false);
        current = cfd.FullHistoryTsLow;
        if (current is not null && ucmp.CompareTimestamp(current, tsLow) > 0)
        {
            // A concurrent caller raised it further.
            throw new RocksDbException(StatusCode.TryAgain, "Operation failed. Try again.: full_history_ts_low: " + Convert.ToHexString(current) +
                                       " is set to be higher than the requested timestamp: " + Convert.ToHexString(tsLow) + "\n");
        }
    }

    /// <summary>
    /// Port of HandleWriteBatchTimestampSizeDifference (util/udt_util.cc, kReconcileInconsistency mode) during WAL
    /// recovery: a column family whose running timestamp size differs from the size recorded in the WAL gets its keys
    /// padded with the minimum timestamp (timestamps enabled since the WAL was written) or stripped (disabled); a
    /// different non-zero size is unrecoverable (InvalidArgument). Returns the rebuilt batch (same sequence number), or
    /// null when the record can be replayed as is.
    /// </summary>
    private byte[]? HandleRecoveredBatchTimestampSizes(ReadOnlySpan<byte> record, IReadOnlyDictionary<uint, int> recorded)
    {
        ColumnFamilyData?[] map = Volatile.Read(ref _cfById);
        bool consistent = true;
        foreach (ColumnFamilyData? c in map)
        {
            if (c is null) continue;
            if (!IsConsistent(c.TimestampSize, recorded, c.Id))
            {
                consistent = false;
                break;
            }
        }
        if (consistent) return null; // AllRunningColumnFamiliesConsistent
        // CheckWriteBatchTimestampSizeConsistency
        var check = new TimestampSizeChecker(map, recorded);
        WriteBatch.Iterate(record, ref check);
        if (!check.NeedsRecovery) return null;
        // TimestampRecoveryHandler
        using var rebuilt = new WriteBatch();
        var h = new TimestampRecoveryHandler(map, recorded, rebuilt);
        WriteBatch.Iterate(record, ref h);
        rebuilt.Sequence = Util.Coding.DecodeFixed64(record);
        return rebuilt.Data.ToArray();
    }

    private static bool IsConsistent(int running, IReadOnlyDictionary<uint, int> recorded, uint id) =>
        recorded.TryGetValue(id, out int r) ? r == running : running == 0;

    /// <summary>GetRecoveryType (util/udt_util.cc).</summary>
    private enum TimestampRecoveryType
    {
        Noop,
        Unrecoverable,
        StripTimestamp,
        PadTimestamp,
    }

    private static TimestampRecoveryType GetRecoveryType(int running, IReadOnlyDictionary<uint, int> recorded, uint id)
    {
        bool hasRecorded = recorded.TryGetValue(id, out int rec);
        if (running == 0) return hasRecorded ? TimestampRecoveryType.StripTimestamp : TimestampRecoveryType.Noop;
        if (!hasRecorded) return TimestampRecoveryType.PadTimestamp;
        return running != rec ? TimestampRecoveryType.Unrecoverable : TimestampRecoveryType.Noop;
    }

    /// <summary>CheckWriteBatchTimestampSizeConsistency (kReconcileInconsistency): dropped column families are ignored.</summary>
    private struct TimestampSizeChecker(ColumnFamilyData?[] map, IReadOnlyDictionary<uint, int> recorded) : IWriteBatchHandler
    {
        public bool NeedsRecovery;

        private void Check(uint id)
        {
            ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
            if (c is null) return; // not running: ignored by recovery
            TimestampRecoveryType t = GetRecoveryType(c.TimestampSize, recorded, id);
            if (t == TimestampRecoveryType.Unrecoverable)
            {
                ThrowHelper.InvalidArgument("WriteBatch contains unrecoverable timestamp size inconsistency.");
            }
            if (t != TimestampRecoveryType.Noop) NeedsRecovery = true;
        }

        public readonly bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Check(columnFamilyId);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Check(columnFamilyId);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => Check(columnFamilyId);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Check(columnFamilyId);
        public readonly void LogData(ReadOnlySpan<byte> blob) { }
        public readonly void MarkBeginPrepare(bool unprepare) { }
        public readonly void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public readonly void MarkNoop(bool emptyBatch) { }
        public readonly void MarkCommit(ReadOnlySpan<byte> xid) { }
        public readonly void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public readonly void MarkRollback(ReadOnlySpan<byte> xid) { }
    }

    /// <summary>
    /// Port of TimestampRecoveryHandler (util/udt_util.cc): copies a batch, stripping or padding (minimum timestamp)
    /// the keys of column families whose timestamp size changed. Log data is not copied (like upstream). Cold path.
    /// </summary>
    private readonly struct TimestampRecoveryHandler(ColumnFamilyData?[] map, IReadOnlyDictionary<uint, int> recorded, WriteBatch dst)
        : IWriteBatchHandler
    {
        private byte[] Reconcile(uint id, ReadOnlySpan<byte> key)
        {
            ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
            if (c is null) return key.ToArray(); // no longer running: copied as is
            switch (GetRecoveryType(c.TimestampSize, recorded, id))
            {
                case TimestampRecoveryType.StripTimestamp:
                    int rec = recorded[id];
                    if (key.Length < rec) ThrowHelper.Corruption("key too short for its recorded timestamp size");
                    return key[..^rec].ToArray();
                case TimestampRecoveryType.PadTimestamp:
                    // AppendKeyWithMinTimestamp (the minimum u64 timestamp is all zeros).
                    byte[] padded = new byte[key.Length + c.TimestampSize];
                    key.CopyTo(padded);
                    return padded;
                case TimestampRecoveryType.Unrecoverable:
                    ThrowHelper.InvalidArgument("Unrecoverable timestamp size inconsistency encountered by TimestampRecoveryHandler.");
                    return [];
                default:
                    return key.ToArray();
            }
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => dst.Put(columnFamilyId, Reconcile(columnFamilyId, key), value);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            dst.TimedPut(columnFamilyId, Reconcile(columnFamilyId, key), value, writeUnixTime);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) =>
            dst.PutEntitySerialized(columnFamilyId, Reconcile(columnFamilyId, key), entity);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.Delete(columnFamilyId, Reconcile(columnFamilyId, key));
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => dst.SingleDelete(columnFamilyId, Reconcile(columnFamilyId, key));
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) =>
            dst.DeleteRange(columnFamilyId, Reconcile(columnFamilyId, beginKey), Reconcile(columnFamilyId, endKey));
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => dst.Merge(columnFamilyId, Reconcile(columnFamilyId, key), value);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) =>
            dst.PutBlobIndex(columnFamilyId, Reconcile(columnFamilyId, key), blobIndex);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare)
        {
            if (unprepare)
            {
                ThrowHelper.InvalidArgument("Handle user defined timestamp setting change is not supported forwrite unprepared policy. The WAL must be emptied.");
            }
            dst.InsertBeginPrepare(writeAfterCommit: true, unpreparedBatch: false);
        }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => dst.InsertEndPrepare(xid);
        public void MarkNoop(bool emptyBatch) => dst.InsertNoop();
        public void MarkCommit(ReadOnlySpan<byte> xid) => dst.MarkCommit(xid);
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => dst.MarkCommitWithTimestamp(xid, commitTimestamp);
        public void MarkRollback(ReadOnlySpan<byte> xid) => dst.MarkRollback(xid);
    }
}
