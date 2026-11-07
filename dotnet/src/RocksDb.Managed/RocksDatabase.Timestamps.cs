using System.Buffers;
using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>User-defined timestamp operations on the default column family (see <see cref="ColumnFamily"/>).</summary>
public sealed partial class RocksDatabase
{
    /// <summary>
    /// Opens the DB and removes, from every column family with user-defined timestamps, all data whose timestamp is
    /// newer than <paramref name="trimTimestamp"/> (DB::OpenAndTrimHistory): each such column family is fully compacted
    /// (bottommost kForceOptimized) with entries and range tombstones above the bound dropped. Requires
    /// <see cref="DbOptions.AvoidFlushDuringRecovery"/> = false (so WAL data is flushed and trimmed too).
    /// </summary>
    public static RocksDatabase OpenAndTrimHistory(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        ReadOnlySpan<byte> trimTimestamp)
    {
        ValueTask<RocksDatabase> t = OpenAndTrimHistoryCoreAsync(path, options, columnFamilies, trimTimestamp.ToArray(), synchronous: true, CancellationToken.None);
        return t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult();
    }

    /// <summary>Async <see cref="OpenAndTrimHistory"/>.</summary>
    public static ValueTask<RocksDatabase> OpenAndTrimHistoryAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        ReadOnlyMemory<byte> trimTimestamp, CancellationToken cancellationToken = default) =>
        OpenAndTrimHistoryCoreAsync(path, options, columnFamilies, trimTimestamp.ToArray(), synchronous: false, cancellationToken);

    private static async ValueTask<RocksDatabase> OpenAndTrimHistoryCoreAsync(string path, DbOptions options,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, byte[] trimTimestamp, bool synchronous, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.AvoidFlushDuringRecovery) ThrowHelper.InvalidArgument("avoid_flush_during_recovery incompatible with OpenAndTrimHistory");
        DbImpl db = await DbImpl.OpenAsync(path, options, columnFamilies, synchronous, ct).ConfigureAwait(false);
        var rdb = new RocksDatabase(db);
        try
        {
            var compact = new CompactRangeOptions { BottommostLevelCompaction = BottommostLevelCompaction.ForceOptimized, TrimTs = trimTimestamp };
            foreach (ColumnFamily cf in rdb.ColumnFamilies)
            {
                // Only compact column families with timestamp enabled.
                int tsSize = cf.Data.TimestampSize;
                if (tsSize == 0) continue;
                if (trimTimestamp.Length != tsSize)
                {
                    ThrowHelper.InvalidArgument("Timestamp size mismatch: trim_ts has " + trimTimestamp.Length + " bytes, column family " + cf.Name
                        + " expects " + tsSize);
                }
                await db.CompactRangeAsync(cf.Data, null, null, compact, synchronous, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            await rdb.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        return rdb;
    }

    /// <summary>Sets <paramref name="key"/> to <paramref name="value"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) => _default.Put(key, timestamp, value);
    public void Put(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> value) =>
        _default.Put(options, key, timestamp, value);
    public ValueTask PutAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> timestamp, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default) =>
        _default.PutAsync(key, timestamp, value, cancellationToken);

    /// <summary>Deletes <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Delete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.Delete(key, timestamp);
    public void Delete(WriteOptions options, ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.Delete(options, key, timestamp);

    /// <summary>SingleDelete of <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void SingleDelete(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp) => _default.SingleDelete(key, timestamp);

    /// <summary>Merge operand for <paramref name="key"/> at <paramref name="timestamp"/> in the default column family.</summary>
    public void Merge(ReadOnlySpan<byte> key, ReadOnlySpan<byte> timestamp, ReadOnlySpan<byte> operand) => _default.Merge(key, timestamp, operand);
    /// <inheritdoc cref="ColumnFamily.DeleteRange(ReadOnlySpan{byte}, ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>
    public void DeleteRange(ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey, ReadOnlySpan<byte> timestamp) => _default.DeleteRange(beginKey, endKey, timestamp);

    /// <inheritdoc cref="ColumnFamily.TryGet(ReadOptions, ReadOnlySpan{byte}, IBufferWriter{byte}, IBufferWriter{byte})"/>
    public bool TryGet(ReadOptions options, ReadOnlySpan<byte> key, IBufferWriter<byte> value, IBufferWriter<byte> timestamp) =>
        _default.TryGet(options, key, value, timestamp);

    /// <inheritdoc cref="ColumnFamily.TryGetAsync(ReadOptions, ReadOnlyMemory{byte}, IBufferWriter{byte}, IBufferWriter{byte}, CancellationToken)"/>
    public ValueTask<bool> TryGetAsync(ReadOptions options, ReadOnlyMemory<byte> key, IBufferWriter<byte> value, IBufferWriter<byte> timestamp,
        CancellationToken cancellationToken = default) => _default.TryGetAsync(options, key, value, timestamp, cancellationToken);

    /// <inheritdoc cref="ColumnFamily.GetFullHistoryTsLow"/>
    public byte[]? GetFullHistoryTsLow() => _default.GetFullHistoryTsLow();

    /// <inheritdoc cref="ColumnFamily.TryGetFullHistoryTsLow(IBufferWriter{byte})"/>
    public bool TryGetFullHistoryTsLow(IBufferWriter<byte> destination) => _default.TryGetFullHistoryTsLow(destination);

    /// <inheritdoc cref="ColumnFamily.IncreaseFullHistoryTsLow"/>
    public void IncreaseFullHistoryTsLow(ReadOnlySpan<byte> tsLow) => _default.IncreaseFullHistoryTsLow(tsLow);

    /// <inheritdoc cref="ColumnFamily.GetNewestUserDefinedTimestamp"/>
    public byte[] GetNewestUserDefinedTimestamp() => _default.GetNewestUserDefinedTimestamp();
}
