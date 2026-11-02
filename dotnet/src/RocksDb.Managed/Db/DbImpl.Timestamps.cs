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
    /// Port of the consistency check of HandleWriteBatchTimestampSizeDifference (util/udt_util.cc) during WAL recovery:
    /// a column family whose running timestamp size differs from the size recorded in the WAL (or recorded but not
    /// running / running but not recorded) would need its keys padded or stripped. Padding/stripping (toggling
    /// timestamps on an existing column family) is not ported: such a batch fails recovery with InvalidArgument
    /// instead of being misread.
    /// </summary>
    private void CheckRecoveredBatchTimestampSizes(ReadOnlySpan<byte> record, IReadOnlyDictionary<uint, int> recorded)
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
        if (consistent) return; // AllRunningColumnFamiliesConsistent
        var h = new TimestampSizeChecker(map, recorded);
        WriteBatch.Iterate(record, ref h);
    }

    private static bool IsConsistent(int running, IReadOnlyDictionary<uint, int> recorded, uint id) =>
        recorded.TryGetValue(id, out int r) ? r == running : running == 0;

    private readonly struct TimestampSizeChecker(ColumnFamilyData?[] map, IReadOnlyDictionary<uint, int> recorded) : IWriteBatchHandler
    {
        private void Check(uint id)
        {
            ColumnFamilyData? c = id < (uint)map.Length ? map[id] : null;
            if (c is null) return; // not running: ignored by recovery
            if (!IsConsistent(c.TimestampSize, recorded, id))
            {
                ThrowHelper.InvalidArgument("WriteBatch contains unrecoverable timestamp size inconsistency.");
            }
        }

        public bool Continue() => true;
        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) => Check(columnFamilyId);
        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity) => Check(columnFamilyId);
        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => Check(columnFamilyId);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => Check(columnFamilyId);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => Check(columnFamilyId);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => Check(columnFamilyId);
        public void LogData(ReadOnlySpan<byte> blob) { }
        public void MarkBeginPrepare(bool unprepare) { }
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) { }
        public void MarkNoop(bool emptyBatch) { }
        public void MarkCommit(ReadOnlySpan<byte> xid) { }
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) { }
        public void MarkRollback(ReadOnlySpan<byte> xid) { }
    }
}
