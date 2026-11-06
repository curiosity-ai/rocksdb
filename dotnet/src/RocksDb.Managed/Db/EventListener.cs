using RocksDb.Managed.Db;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>
/// Port of rocksdb::BackgroundErrorReason (include/rocksdb/listener.h): the background operation that failed. Values
/// are kept identical.
/// </summary>
public enum BackgroundErrorReason
{
    Flush = 0,
    Compaction = 1,
    WriteCallback = 2,
    MemTable = 3,
    ManifestWrite = 4,
    FlushNoWal = 5,
    ManifestWriteNoWal = 6,
    AsyncFileOpen = 7,
}

/// <summary>Port of rocksdb::FlushReason (include/rocksdb/listener.h). Values are kept identical.</summary>
public enum FlushReason
{
    Others = 0x00,
    GetLiveFiles = 0x01,
    ShutDown = 0x02,
    ExternalFileIngestion = 0x03,
    ManualCompaction = 0x04,
    WriteBufferManager = 0x05,
    WriteBufferFull = 0x06,
    Test = 0x07,
    DeleteFiles = 0x08,
    AutoCompaction = 0x09,
    ManualFlush = 0x0a,
    ErrorRecovery = 0x0b,
    ErrorRecoveryRetryFlush = 0x0c,
    WalFull = 0x0d,
    CatchUpAfterErrorRecovery = 0x0e,
}

/// <summary>Port of rocksdb::TableFileCreationReason (include/rocksdb/listener.h). Values are kept identical.</summary>
public enum TableFileCreationReason
{
    Flush = 0,
    Compaction = 1,
    Recovery = 2,
    Misc = 3,
}

/// <summary>Port of rocksdb::FlushJobInfo: a flush that started or completed.</summary>
public sealed class FlushJobInfo
{
    public uint ColumnFamilyId { get; init; }
    public required string ColumnFamilyName { get; init; }
    /// <summary>Path of the L0 file being created (it may not exist yet in OnFlushBegin; empty if the flush produced no file).</summary>
    public required string FilePath { get; init; }
    public ulong FileNumber { get; init; }
    /// <summary>Id of the managed thread that runs the flush.</summary>
    public int ThreadId { get; init; }
    public int JobId { get; init; }
    /// <summary>Writes were slowed down when the flush started/ended (too many L0 files or memtables).</summary>
    public bool TriggeredWritesSlowdown { get; init; }
    /// <summary>Writes were stopped when the flush started/ended.</summary>
    public bool TriggeredWritesStop { get; init; }
    public ulong SmallestSeqno { get; init; }
    public ulong LargestSeqno { get; init; }
    /// <summary>Properties of the created table (OnFlushCompleted only; null when no file was created).</summary>
    public TableProperties? TableProperties { get; init; }
    public FlushReason FlushReason { get; init; }
}

/// <summary>Port of rocksdb::CompactionJobInfo.</summary>
public sealed class CompactionJobInfo
{
    public uint ColumnFamilyId { get; init; }
    public required string ColumnFamilyName { get; init; }
    /// <summary>Null on success (OnCompactionBegin always null), otherwise the failure.</summary>
    public RocksDbException? Status { get; init; }
    public int ThreadId { get; init; }
    public int JobId { get; init; }
    /// <summary>Smallest input level.</summary>
    public int BaseInputLevel { get; init; }
    public int OutputLevel { get; init; }
    /// <summary>Full paths of the input files.</summary>
    public required IReadOnlyList<string> InputFiles { get; init; }
    /// <summary>Full paths of the output files (empty in OnCompactionBegin).</summary>
    public required IReadOnlyList<string> OutputFiles { get; init; }
    public CompactionReason CompactionReason { get; init; }
    public ulong TotalInputBytes { get; init; }
    public ulong TotalOutputBytes { get; init; }
    public long NumInputRecords { get; init; }
    public long NumOutputRecords { get; init; }
    /// <summary>The compaction only moved files to another level (no rewrite).</summary>
    public bool IsTrivialMove { get; init; }
}

/// <summary>Port of rocksdb::TableFileCreationInfo.</summary>
public sealed class TableFileCreationInfo
{
    public required string DbName { get; init; }
    public required string ColumnFamilyName { get; init; }
    public required string FilePath { get; init; }
    public ulong FileSize { get; init; }
    public int JobId { get; init; }
    public TableFileCreationReason Reason { get; init; }
    /// <summary>Null when the file was created successfully, otherwise the failure (the file was deleted).</summary>
    public RocksDbException? Status { get; init; }
    public TableProperties? TableProperties { get; init; }
}

/// <summary>Port of rocksdb::TableFileDeletionInfo.</summary>
public sealed class TableFileDeletionInfo
{
    public required string DbName { get; init; }
    public required string FilePath { get; init; }
    public int JobId { get; init; }
    /// <summary>Null when the file was deleted, otherwise the failure.</summary>
    public RocksDbException? Status { get; init; }
}

/// <summary>
/// Argument of <see cref="EventListener.OnBackgroundError"/> (rocksdb::EventListener::OnBackgroundError's
/// <c>Status* bg_error</c>). Setting <see cref="Error"/> to null suppresses the error: the DB keeps working as if the
/// background operation had not failed (use with care, the failed operation is not retried).
/// </summary>
public sealed class BackgroundErrorEventArgs
{
    internal BackgroundErrorEventArgs(BackgroundErrorReason reason, RocksDbException error)
    {
        Reason = reason;
        Error = error;
    }

    public BackgroundErrorReason Reason { get; }

    /// <summary>The error, with its <see cref="RocksDbException.Severity"/> as classified by the DB. Settable; null suppresses.</summary>
    public RocksDbException? Error { get; set; }
}

/// <summary>
/// Argument of <see cref="EventListener.OnErrorRecoveryBegin"/> (rocksdb::EventListener::OnErrorRecoveryBegin):
/// automatic recovery is about to start; clearing <see cref="AutoRecovery"/> disables it (the DB then waits for
/// <see cref="RocksDatabase.Resume"/>).
/// </summary>
public sealed class ErrorRecoveryBeginEventArgs
{
    internal ErrorRecoveryBeginEventArgs(BackgroundErrorReason reason, RocksDbException error)
    {
        Reason = reason;
        Error = error;
    }

    public BackgroundErrorReason Reason { get; }
    public RocksDbException Error { get; }
    public bool AutoRecovery { get; set; } = true;
}

/// <summary>Port of rocksdb::BackgroundErrorRecoveryInfo.</summary>
public sealed class BackgroundErrorRecoveryInfo
{
    /// <summary>The background error that was being recovered from.</summary>
    public required RocksDbException OldBackgroundError { get; init; }

    /// <summary>
    /// Null when the recovery succeeded (the DB accepts writes again); otherwise the reason it ended without success
    /// (still-set background error, shutdown (ShutdownInProgress) or exceeded retries (Aborted)).
    /// </summary>
    public RocksDbException? NewBackgroundError { get; init; }
}

/// <summary>Port of rocksdb::WriteStallInfo.</summary>
public sealed class WriteStallInfo
{
    public required string ColumnFamilyName { get; init; }
    public WriteStallCondition Current { get; init; }
    public WriteStallCondition Previous { get; init; }
}

/// <summary>Port of rocksdb::MemTableInfo: a memtable that became immutable.</summary>
public sealed class MemTableInfo
{
    public required string ColumnFamilyName { get; init; }
    /// <summary>Sequence number of the first element inserted (0 if empty).</summary>
    public ulong FirstSeqno { get; init; }
    /// <summary>Sequence number guaranteed to be smaller than or equal to any key of the memtable.</summary>
    public ulong EarliestSeqno { get; init; }
    public ulong NumEntries { get; init; }
    public ulong NumDeletes { get; init; }
}

/// <summary>Port of rocksdb::ExternalFileIngestionInfo.</summary>
public sealed class ExternalFileIngestionInfo
{
    public required string ColumnFamilyName { get; init; }
    /// <summary>Path of the file outside the DB.</summary>
    public required string ExternalFilePath { get; init; }
    /// <summary>Path of the file inside the DB.</summary>
    public required string InternalFilePath { get; init; }
    /// <summary>Global sequence number assigned to the keys of the file (0 if none).</summary>
    public ulong GlobalSeqno { get; init; }
    public TableProperties? TableProperties { get; init; }
}

/// <summary>
/// Port of rocksdb::EventListener (include/rocksdb/listener.h): callbacks for flush, compaction, table file, write
/// stall, memtable, ingestion, column family handle and background error events. Register instances in
/// <see cref="DbOptions.Listeners"/>.
/// <para>
/// <b>Threading</b>: callbacks run on the DB's background threads (or a dedicated notification task for write stall
/// and memtable events), never on the read/write hot path and never while the DB mutex is held. They must be
/// thread-safe and should return quickly: a slow callback delays the background job that raised it. A callback must
/// not wait for a flush or compaction to finish (deadlock risk), but it may read from or write to the DB. Exceptions
/// thrown by callbacks are caught and logged.
/// </para>
/// </summary>
public abstract class EventListener
{
    /// <summary>A flush job is about to write an L0 file (EventListener::OnFlushBegin).</summary>
    public virtual void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
    {
    }

    /// <summary>A flush job installed its result (EventListener::OnFlushCompleted).</summary>
    public virtual void OnFlushCompleted(RocksDatabase? db, FlushJobInfo info)
    {
    }

    /// <summary>A compaction is about to run (EventListener::OnCompactionBegin; also for trivial moves).</summary>
    public virtual void OnCompactionBegin(RocksDatabase? db, CompactionJobInfo info)
    {
    }

    /// <summary>A compaction finished, successfully or not (EventListener::OnCompactionCompleted).</summary>
    public virtual void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info)
    {
    }

    /// <summary>A flush or compaction finished writing a table file (EventListener::OnTableFileCreated).</summary>
    public virtual void OnTableFileCreated(TableFileCreationInfo info)
    {
    }

    /// <summary>An obsolete table file was deleted (EventListener::OnTableFileDeleted).</summary>
    public virtual void OnTableFileDeleted(TableFileDeletionInfo info)
    {
    }

    /// <summary>
    /// A background operation failed (EventListener::OnBackgroundError). The error can be replaced or suppressed through
    /// <see cref="BackgroundErrorEventArgs.Error"/>.
    /// </summary>
    public virtual void OnBackgroundError(BackgroundErrorEventArgs e)
    {
    }

    /// <summary>
    /// Automatic recovery from a background error is about to start (EventListener::OnErrorRecoveryBegin); it can be
    /// disabled through <see cref="ErrorRecoveryBeginEventArgs.AutoRecovery"/>.
    /// </summary>
    public virtual void OnErrorRecoveryBegin(ErrorRecoveryBeginEventArgs e)
    {
    }

    /// <summary>A recovery (automatic or <see cref="RocksDatabase.Resume"/>) ended (EventListener::OnErrorRecoveryEnd).</summary>
    public virtual void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
    {
    }

    /// <summary>The write stall condition of a column family changed (EventListener::OnStallConditionsChanged).</summary>
    public virtual void OnStallConditionsChanged(WriteStallInfo info)
    {
    }

    /// <summary>A memtable became immutable and was queued for flush (EventListener::OnMemTableSealed).</summary>
    public virtual void OnMemTableSealed(MemTableInfo info)
    {
    }

    /// <summary>
    /// A column family handle is being released (EventListener::OnColumnFamilyHandleDeletionStarted). In the managed
    /// port handles live as long as the DB, so this is called when a column family is dropped and, for every remaining
    /// handle, when the DB is closed.
    /// </summary>
    public virtual void OnColumnFamilyHandleDeletionStarted(ColumnFamily handle)
    {
    }

    /// <summary>An external SST file was ingested (EventListener::OnExternalFileIngested).</summary>
    public virtual void OnExternalFileIngested(RocksDatabase? db, ExternalFileIngestionInfo info)
    {
    }

    /// <summary>
    /// The DB begins shutting down (EventListener::OnDBShutdownBegin): called once per DB, by
    /// <see cref="RocksDatabase.CancelAllBackgroundWork"/> or by close / dispose, before the shutdown state is published.
    /// Also called while cleaning up a failed open, with a null <paramref name="db"/> (the DB object was never
    /// returned). Background work may still be running and other callbacks may still run concurrently or afterwards;
    /// implementations should not call blocking DB APIs.
    /// </summary>
    public virtual void OnDBShutdownBegin(RocksDatabase? db)
    {
    }
}
