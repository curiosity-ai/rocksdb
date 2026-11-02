using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>
/// User-defined timestamp operations of a column family whose comparator carries a timestamp
/// (<see cref="Comparator.TimestampSize"/> &gt; 0, e.g. <see cref="Comparator.BytewiseWithU64Ts"/>). Port of the
/// timestamp overloads of DB::Put / Delete / SingleDelete / Merge, DB::Get with ReadOptions::timestamp,
/// DB::IncreaseFullHistoryTsLow and DB::GetFullHistoryTsLow (include/rocksdb/db.h, db/db_impl/db_impl_write.cc).
/// </summary>
/// <remarks>
/// Every key of such a column family is stored as <c>key | timestamp</c>. Writes must use the overloads taking a
/// timestamp (the others throw InvalidArgument), reads must set <see cref="ReadOptions.Timestamp"/>: they see, per key,
/// the newest version whose timestamp is &lt;= the read timestamp. <see cref="ReadOptions.IterStartTs"/> makes an
/// iterator return every version in [IterStartTs, Timestamp], deletions included.
/// </remarks>
public sealed partial class ColumnFamily
{
    /// <summary>Size in bytes of the user-defined timestamp of this column family (0 when timestamps are disabled).</summary>
    public int TimestampSize => _cfd.TimestampSize;

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowCfHasTimestamp() =>
        // DBImpl::FailIfCfHasTs
        ThrowHelper.InvalidArgument("cannot call this method on column family " + Name + " that enables timestamp");

    /// <summary>Port of DBImpl::FailIfTsMismatchCf.</summary>
    internal static void FailIfTsMismatchCf(ColumnFamilyData cfd, int timestampLength)
    {
        if (cfd.TimestampSize == 0) ThrowHelper.InvalidArgument("cannot call this method on column family " + cfd.Name + " that does not enable timestamp");
        if (timestampLength != cfd.TimestampSize)
        {
            ThrowHelper.InvalidArgument("Timestamp sizes mismatch: expect " + cfd.TimestampSize.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                                        ", " + timestampLength.ToString(System.Globalization.CultureInfo.InvariantCulture) + " given");
        }
    }

    private void WriteOneWithTimestamp(WriteOptions? options, Op op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value)
    {
        FailIfTsMismatchCf(_cfd, timestamp.Length);
        WriteBatch b = WriteBatch.Rent();
        try
        {
            EncodeWithTimestamp(b, op, key, timestamp, value);
            ValueTask t = _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: true, CancellationToken.None);
            t.GetAwaiter().GetResult();
        }
        finally
        {
            b.Dispose();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteOneWithTimestampAsync(WriteOptions? options, Op op, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp,
        ReadOnlyMemory<byte> value, CancellationToken ct)
    {
        FailIfTsMismatchCf(_cfd, timestamp.Length);
        WriteBatch b = WriteBatch.Rent();
        try
        {
            EncodeWithTimestamp(b, op, key.Span, timestamp.Span, value.Span);
            await _db.WriteAsync(options, b, validateColumnFamilies: false, synchronous: false, ct).ConfigureAwait(false);
        }
        finally
        {
            b.Dispose();
        }
    }

    private void EncodeWithTimestamp(WriteBatch b, Op op, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value)
    {
        uint id = _cfd.Id;
        switch (op)
        {
            case Op.Put: b.Put(id, key, timestamp, value); break;
            case Op.Delete: b.Delete(id, key, timestamp); break;
            case Op.SingleDelete: b.SingleDelete(id, key, timestamp); break;
            case Op.Merge:
                if (_cfd.MergeOperator is null) ThrowHelper.NotSupported("Provide a merge_operator when opening DB");
                b.Merge(id, key, timestamp, value);
                break;
            default:
                // Range tombstones with timestamps are not supported by the managed read path yet.
                ThrowHelper.NotSupported("DeleteRange with user-defined timestamps");
                break;
        }
    }

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/> at <paramref name="timestamp"/> (DB::Put(cf, key, ts, value)).</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) => WriteOneWithTimestamp(null, Op.Put, key, timestamp, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) =>
        WriteOneWithTimestamp(options, Op.Put, key, timestamp, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        WriteOneWithTimestampAsync(null, Op.Put, key, timestamp, value, cancellationToken);
    public ValueTask PutAsync(WriteOptions options, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp, ReadOnlyMemory<byte> value,
        CancellationToken cancellationToken = default) =>
        WriteOneWithTimestampAsync(options, Op.Put, key, timestamp, value, cancellationToken);

    /// <summary>Deletes <paramref name="key"/> at <paramref name="timestamp"/> (DB::Delete(cf, key, ts)).</summary>
    public void Delete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => WriteOneWithTimestamp(null, Op.Delete, key, timestamp, default);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => WriteOneWithTimestamp(options, Op.Delete, key, timestamp, default);
    public ValueTask DeleteAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp, CancellationToken cancellationToken = default) =>
        WriteOneWithTimestampAsync(null, Op.Delete, key, timestamp, default, cancellationToken);

    /// <summary>SingleDelete of <paramref name="key"/> at <paramref name="timestamp"/> (DB::SingleDelete(cf, key, ts)).</summary>
    public void SingleDelete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => WriteOneWithTimestamp(null, Op.SingleDelete, key, timestamp, default);
    public void SingleDelete(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) =>
        WriteOneWithTimestamp(options, Op.SingleDelete, key, timestamp, default);

    /// <summary>Adds a merge operand for <paramref name="key"/> at <paramref name="timestamp"/> (DB::Merge(cf, key, ts, value)).</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> operand) => WriteOneWithTimestamp(null, Op.Merge, key, timestamp, operand);
    public void Merge(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> operand) =>
        WriteOneWithTimestamp(options, Op.Merge, key, timestamp, operand);

    /// <summary>
    /// Looks up <paramref name="key"/> at <see cref="ReadOptions.Timestamp"/> (required), appending the value to
    /// <paramref name="value"/> and the timestamp of the version found to <paramref name="timestamp"/>
    /// (DB::Get(..., std::string* timestamp)). Returns false if the key does not exist at that timestamp; the timestamp of
    /// a deletion that hides the key is still reported (as upstream).
    /// </summary>
    public bool TryGet(ReadOptions options, ReadOnlySpan<byte> key, IBufferWriter<byte> value, IBufferWriter<byte> timestamp)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(timestamp);
        GetContext ctx = Lookup(options, key);
        try
        {
            if (ctx.HasTimestamp) timestamp.Write(ctx.FoundTimestamp);
            if (!ctx.TryResolve(key, out ReadOnlySpan<byte> v)) return false;
            value.Write(v);
            return true;
        }
        finally
        {
            GetContext.Return(ctx);
        }
    }

    /// <summary>
    /// The current full_history_ts_low of this column family (DB::GetFullHistoryTsLow), or null when it was never set.
    /// Reads with a timestamp below it fail with InvalidArgument; compaction may collapse versions older than it.
    /// </summary>
    public byte[]? GetFullHistoryTsLow()
    {
        _db.ThrowIfDisposed();
        if (_cfd.TimestampSize == 0) ThrowHelper.InvalidArgument("Timestamp is not enabled in this column family");
        byte[]? ts = _cfd.FullHistoryTsLow;
        return ts is null ? null : (byte[])ts.Clone();
    }

    /// <summary>
    /// Raises full_history_ts_low to <paramref name="tsLow"/> (DB::IncreaseFullHistoryTsLow): recorded in the MANIFEST.
    /// Throws InvalidArgument when it would decrease the current value or the size does not match.
    /// </summary>
    public void IncreaseFullHistoryTsLow(ReadOnlySpan<byte> tsLow)
    {
        _db.ThrowIfDisposed();
        _db.IncreaseFullHistoryTsLowAsync(_cfd, tsLow.ToArray(), synchronous: true, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Port of DB::GetNewestUserDefinedTimestamp: only defined when user-defined timestamps are not persisted
    /// (persist_user_defined_timestamps = false, memtable-only timestamps), which this port does not support; throws
    /// InvalidArgument without timestamps and NotSupported otherwise (as upstream does for persisted timestamps).
    /// </summary>
    public byte[] GetNewestUserDefinedTimestamp()
    {
        _db.ThrowIfDisposed();
        if (_cfd.TimestampSize == 0) ThrowHelper.InvalidArgument("Timestamp is not enabled in this column family");
        throw RocksDbException.NotSupported("GetNewestUserDefinedTimestamp doesn't support the case when user defined timestamps are persisted.");
    }

    /// <summary>Async <see cref="IncreaseFullHistoryTsLow"/>.</summary>
    public ValueTask IncreaseFullHistoryTsLowAsync(ReadOnlyMemory<byte> tsLow, CancellationToken cancellationToken = default)
    {
        _db.ThrowIfDisposed();
        return _db.IncreaseFullHistoryTsLowAsync(_cfd, tsLow.ToArray(), synchronous: false, cancellationToken);
    }
}
