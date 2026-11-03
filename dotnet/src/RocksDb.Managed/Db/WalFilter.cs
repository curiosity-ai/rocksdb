using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>
/// What recovery does with a WAL record after <see cref="WalFilter.LogRecordFound"/>. Port of
/// <c>WalFilter::WalProcessingOption</c> (include/rocksdb/wal_filter.h); the values match the C API's
/// <c>rocksdb_wal_filter_*</c> constants.
/// </summary>
public enum WalProcessingOption
{
    /// <summary>Continue processing as usual.</summary>
    ContinueProcessing = 0,
    /// <summary>Ignore the current record but continue processing of the log(s).</summary>
    IgnoreCurrentRecord = 1,
    /// <summary>
    /// Stop replay of the logs and discard them: the current record and every later record (also in later WALs) is
    /// dropped. Once the open completes, the discarded logs are not replayed on subsequent recoveries.
    /// </summary>
    StopReplay = 2,
    /// <summary>
    /// The filter found a corrupted record. Handled like a corruption found while reading the WAL, according to
    /// <see cref="DbOptions.WalRecoveryMode"/> (e.g. the open fails with kTolerateCorruptedTailRecords and
    /// kAbsoluteConsistency, replay stops with kPointInTimeRecovery, the rest of the WAL is skipped with
    /// kSkipAnyCorruptedRecords). Without <see cref="DbOptions.ParanoidChecks"/> the report is ignored and the record is
    /// replayed.
    /// </summary>
    CorruptedRecord = 3,
}

/// <summary>
/// Lets an application inspect WAL records during recovery (DB open), drop or rewrite them, report them as corrupted or
/// stop the replay. Set it with <see cref="DbOptions.WalFilter"/>. Port of <c>rocksdb::WalFilter</c>
/// (include/rocksdb/wal_filter.h), invoked like DBImpl::InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap /
/// InvokeWalFilterIfNeededOnWalRecord (db/db_impl/db_impl_open.cc).
/// <para>
/// Called on the thread that opens the DB, by primary and read-only opens (secondary instances do not replay WALs
/// through the filter, like upstream). Records already flushed to table files for a column family are still passed to
/// the filter (its WAL is replayed for the other column families); <see cref="ColumnFamilyLogNumberMap"/> tells the
/// filter which WALs each column family still needs. An exception thrown by the filter fails the open.
/// </para>
/// </summary>
public abstract class WalFilter
{
    /// <summary>Identifies the filter in the info LOG (<c>Options.wal_filter</c>). Not persisted.</summary>
    public abstract string Name { get; }

    /// <summary>
    /// Called once before the WALs are replayed with the log number of each column family (a WAL with a smaller number
    /// is already flushed to table files for that column family) and the id of each column family name (only names are
    /// known during recovery, while write batch records name column families by id). Default: does nothing.
    /// </summary>
    /// <param name="columnFamilyLogNumbers">Column family id to log number.</param>
    /// <param name="columnFamilyNameIds">Column family name to column family id.</param>
    public virtual void ColumnFamilyLogNumberMap(IReadOnlyDictionary<uint, ulong> columnFamilyLogNumbers,
        IReadOnlyDictionary<string, uint> columnFamilyNameIds)
    {
    }

    /// <summary>
    /// Called for each WAL record found during the replay. Default: forwards to the legacy <see cref="LogRecord"/>.
    /// </summary>
    /// <param name="logNumber">Number of the WAL the record is in.</param>
    /// <param name="logFileName">Path of that WAL (informational).</param>
    /// <param name="batch">The record. Only valid during the call; must not be modified.</param>
    /// <param name="newBatch">
    /// An empty batch the filter may fill with a replacement for the record (e.g. to drop or alter some of its entries);
    /// takes effect only when <paramref name="batchChanged"/> is set. It must not hold more entries than
    /// <paramref name="batch"/>: the open then fails with NotSupported. It replays with the sequence number of the
    /// original record. Only valid during the call.
    /// </param>
    /// <param name="batchChanged">Set to true when <paramref name="newBatch"/> replaces the record.</param>
    /// <returns>How recovery processes the record.</returns>
    public virtual WalProcessingOption LogRecordFound(ulong logNumber, string logFileName, WriteBatch batch, WriteBatch newBatch, out bool batchChanged) =>
        LogRecord(batch, newBatch, out batchChanged);

    /// <summary>
    /// Legacy form of <see cref="LogRecordFound"/> without the WAL number and name, kept for compatibility; new filters
    /// should override <see cref="LogRecordFound"/>. Default: <see cref="WalProcessingOption.ContinueProcessing"/>.
    /// </summary>
    public virtual WalProcessingOption LogRecord(WriteBatch batch, WriteBatch newBatch, out bool batchChanged)
    {
        batchChanged = false;
        return WalProcessingOption.ContinueProcessing;
    }
}
