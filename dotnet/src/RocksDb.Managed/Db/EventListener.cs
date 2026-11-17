using RocksDb.Managed.Compression;
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
/// Port of rocksdb::BlobFileCreationReason (include/rocksdb/types.h): why a blob file was written. Values are kept
/// identical.
/// </summary>
public enum BlobFileCreationReason : byte
{
    Flush = 0,
    Compaction = 1,
    Recovery = 2,
}

/// <summary>Port of rocksdb::TableFileCreationBriefInfo: a table file that is about to be created.</summary>
public sealed class TableFileCreationBriefInfo
{
    public required string DbName { get; init; }
    public required string ColumnFamilyName { get; init; }
    /// <summary>Path of the file being created (it may not exist yet).</summary>
    public required string FilePath { get; init; }
    public int JobId { get; init; }
    public TableFileCreationReason Reason { get; init; }
}

/// <summary>Port of rocksdb::BlobFileCreationBriefInfo: a blob file that is about to be created.</summary>
public sealed class BlobFileCreationBriefInfo
{
    public required string DbName { get; init; }
    public required string ColumnFamilyName { get; init; }
    /// <summary>Path of the blob file being created (it may not exist yet).</summary>
    public required string FilePath { get; init; }
    public int JobId { get; init; }
    public BlobFileCreationReason Reason { get; init; }
}

/// <summary>Port of rocksdb::BlobFileCreationInfo: a blob file that was finished (or failed).</summary>
public sealed class BlobFileCreationInfo
{
    public required string DbName { get; init; }
    public required string ColumnFamilyName { get; init; }
    public required string FilePath { get; init; }
    public int JobId { get; init; }
    public BlobFileCreationReason Reason { get; init; }
    /// <summary>Number of blobs in the file.</summary>
    public ulong TotalBlobCount { get; init; }
    /// <summary>Bytes of the blob records in the file (headers, keys and values; not the file header and footer).</summary>
    public ulong TotalBlobBytes { get; init; }
    /// <summary>Null when the file was created successfully, otherwise the failure.</summary>
    public RocksDbException? Status { get; init; }
    /// <summary>The file checksum, hex encoded (kUnknownFileChecksum, empty, without a checksum generator).</summary>
    public required string FileChecksum { get; init; }
    /// <summary>The checksum function name ("Unknown" without a checksum generator).</summary>
    public required string FileChecksumFuncName { get; init; }
}

/// <summary>Port of rocksdb::BlobFileDeletionInfo.</summary>
public sealed class BlobFileDeletionInfo
{
    public required string DbName { get; init; }
    public required string FilePath { get; init; }
    public int JobId { get; init; }
    /// <summary>Null when the file was deleted, otherwise the failure.</summary>
    public RocksDbException? Status { get; init; }
}

/// <summary>Port of rocksdb::ManualFlushInfo: a column family whose flush was requested by a manual flush.</summary>
public sealed class ManualFlushInfo
{
    public uint ColumnFamilyId { get; init; }
    public required string ColumnFamilyName { get; init; }
    public FlushReason FlushReason { get; init; }
}

/// <summary>
/// Port of rocksdb::SubcompactionJobInfo: one subcompaction (key range) of a compaction. A compaction that is not split
/// still runs as one subcompaction with <see cref="SubcompactionJobId"/> 0.
/// </summary>
public sealed class SubcompactionJobInfo
{
    public uint ColumnFamilyId { get; init; }
    public required string ColumnFamilyName { get; init; }
    /// <summary>Null on success (OnSubcompactionBegin always null), otherwise the subcompaction's failure.</summary>
    public RocksDbException? Status { get; init; }
    /// <summary>Id of the managed thread that runs the subcompaction.</summary>
    public int ThreadId { get; init; }
    /// <summary>The compaction's job id (the same as the <see cref="CompactionJobInfo.JobId"/> of its compaction).</summary>
    public int JobId { get; init; }
    /// <summary>Unique only within the compaction identified by <see cref="JobId"/>.</summary>
    public int SubcompactionJobId { get; init; }
    /// <summary>The compaction's start level (Compaction::start_level).</summary>
    public int BaseInputLevel { get; init; }
    public int OutputLevel { get; init; }
    public CompactionReason CompactionReason { get; init; }
    /// <summary>Compression of the output tables.</summary>
    public CompressionType Compression { get; init; }
    /// <summary>Compression of the blob files written (blob_compression_type).</summary>
    public CompressionType BlobCompressionType { get; init; }
    // The subcompaction's CompactionJobStats (SubcompactionJobInfo::stats): upstream fills only its input side
    // (CompactionJob::FinalizeSubcompactionJobStats); the output side is aggregated for the whole compaction, see
    // CompactionJobInfo. All zero in OnSubcompactionBegin.

    /// <summary>Entries read by this subcompaction (CompactionJobStats::num_input_records).</summary>
    public long NumInputRecords { get; init; }
    /// <summary>Deletions read (num_input_deletion_records).</summary>
    public long NumInputDeletionRecords { get; init; }
    /// <summary>Raw (uncompressed) bytes of the keys read (total_input_raw_key_bytes).</summary>
    public long TotalInputRawKeyBytes { get; init; }
    /// <summary>Raw (uncompressed) bytes of the values read (total_input_raw_value_bytes).</summary>
    public long TotalInputRawValueBytes { get; init; }
    /// <summary>SingleDelete records that fell through without a matching Put (num_single_del_fallthru).</summary>
    public long NumSingleDelFallthru { get; init; }
    /// <summary>SingleDelete records that met a non-Put (num_single_del_mismatch).</summary>
    public long NumSingleDelMismatch { get; init; }
    /// <summary>CPU time of the subcompaction's thread (cpu_micros; 0 where the platform has no thread CPU clock).</summary>
    public ulong CpuMicros { get; init; }
}

/// <summary>
/// Port of rocksdb::FileOperationType (include/rocksdb/listener.h): the operation of a <see cref="FileOperationInfo"/> or
/// an <see cref="IOErrorInfo"/>. Values are kept identical.
/// </summary>
public enum FileOperationType
{
    Read = 0,
    Write = 1,
    Truncate = 2,
    Close = 3,
    Flush = 4,
    Sync = 5,
    Fsync = 6,
    RangeSync = 7,
    Append = 8,
    PositionedAppend = 9,
    Open = 10,
    Verify = 11,
}

/// <summary>
/// Port of rocksdb::FileOperationInfo: a finished file operation, passed by reference to the OnFile*Finish callbacks of
/// listeners whose <see cref="EventListener.ShouldBeNotifiedOnFileIO"/> returns true. A value type: reporting an
/// operation does not allocate. Copy what you need, the instance is only valid during the callback.
/// </summary>
public readonly struct FileOperationInfo
{
    internal FileOperationInfo(FileOperationType type, string path, Temperature temperature, ulong offset, ulong length,
        DateTime startTime, TimeSpan duration, RocksDbException? status)
    {
        Type = type;
        Path = path;
        Temperature = temperature;
        Offset = offset;
        Length = length;
        StartTime = startTime;
        Duration = duration;
        Status = status;
    }

    public FileOperationType Type { get; }
    /// <summary>Full path of the file.</summary>
    public string Path { get; }
    /// <summary>The file's temperature when known (best effort, as upstream), else <see cref="Temperature.Unknown"/>.</summary>
    public Temperature Temperature { get; }
    /// <summary>Offset of a read, write or range sync (0 for the other operations).</summary>
    public ulong Offset { get; }
    /// <summary>Bytes read (which can be fewer than requested at the end of the file), written or range-synced (0 for the other operations).</summary>
    public ulong Length { get; }
    /// <summary>When the operation started (UTC, system clock).</summary>
    public DateTime StartTime { get; }
    /// <summary>How long the operation took (monotonic clock).</summary>
    public TimeSpan Duration { get; }
    /// <summary>Null when the operation succeeded, otherwise its failure.</summary>
    public RocksDbException? Status { get; }
}

/// <summary>
/// Port of rocksdb::IOErrorInfo: a file operation that failed, passed to <see cref="EventListener.OnIOError"/> of the
/// listeners whose <see cref="EventListener.ShouldBeNotifiedOnFileIO"/> returns true.
/// </summary>
public sealed class IOErrorInfo
{
    internal IOErrorInfo(RocksDbException status, FileOperationType operation, string filePath, ulong length, ulong offset)
    {
        Status = status;
        Operation = operation;
        FilePath = filePath;
        Length = length;
        Offset = offset;
    }

    public RocksDbException Status { get; }
    /// <summary>
    /// The failed operation: <see cref="FileOperationType.Read"/>, <see cref="FileOperationType.Append"/> (a buffered write),
    /// <see cref="FileOperationType.PositionedAppend"/> (a direct I/O write), <see cref="FileOperationType.Flush"/>,
    /// <see cref="FileOperationType.Sync"/>, <see cref="FileOperationType.RangeSync"/>, <see cref="FileOperationType.Close"/>
    /// or <see cref="FileOperationType.Verify"/> (the MANIFEST checks on close).
    /// </summary>
    public FileOperationType Operation { get; }
    public string FilePath { get; }
    public ulong Length { get; }
    public ulong Offset { get; }
}

/// <summary>
/// Port of rocksdb::EventListener (include/rocksdb/listener.h): callbacks for flush, compaction, subcompaction, table
/// and blob file, write stall, memtable, ingestion, column family handle, background error and file I/O events. Register
/// instances in <see cref="DbOptions.Listeners"/>.
/// <para>
/// <b>Threading</b>: callbacks run on the thread of the event, like upstream's: the DB's background threads for flushes,
/// compactions and their files, a dedicated notification task for write stall and memtable events, the calling
/// thread for manual flush scheduling and ingestion. None of these run on the read/write hot path or while the DB mutex
/// is held. The file I/O callbacks (<see cref="OnFileReadFinish"/> ... <see cref="OnIOError"/>) are the exception: they
/// run on the thread that did the I/O, including threads reading or writing the DB, so they must be cheap. Callbacks
/// must be thread-safe and should return quickly: a slow callback delays the operation that raised it. A callback must
/// not wait for a flush or compaction to finish (deadlock risk), but it may read from or write to the DB (except from the
/// file I/O callbacks, which may run inside a DB operation). Exceptions thrown by callbacks are caught and logged; the
/// operation that raised the event goes on as if the callback had returned.
/// </para>
/// <para>
/// <b>File I/O</b>: the OnFile*Finish callbacks and <see cref="OnIOError"/> are only called for listeners whose
/// <see cref="ShouldBeNotifiedOnFileIO"/> returned true when the DB was opened. They cover the files upstream reports
/// (table files read and written by the DB, blob files, WAL files written, MANIFEST files read and written, WAL files read
/// by GetUpdatesSince), not the info LOG, OPTIONS, CURRENT or the files read by ingestion, import, backup, checkpoint,
/// repair, <see cref="SstFileWriter"/> or <see cref="Table.SstFileReader"/>. When no listener asks for them they cost
/// nothing: the files are not wrapped.
/// </para>
/// <para>
/// <b>Error recovery</b>: <see cref="OnBackgroundError"/> and <see cref="OnErrorRecoveryBegin"/> run on the thread whose
/// operation failed: a flush or compaction (including the flushes an error recovery runs), a write or memtable switch
/// that failed its WAL write, or the error recovery itself. <see cref="OnErrorRecoveryEnd"/> runs on the recovery: the
/// automatic recovery task, the <see cref="SstFileManager"/>'s out-of-space recovery task or the thread calling
/// <see cref="RocksDatabase.Resume"/> (during close, also the thread that reported an error the recovery was refused for).
/// <list type="bullet">
/// <item>From <see cref="OnErrorRecoveryEnd"/> raised by a recovery, the DB may be closed (<see cref="RocksDatabase.Dispose"/>,
/// or <see cref="RocksDatabase.DisposeAsync"/>, also when blocking on it): the close does not wait for the recovery task
/// that raised the callback, which has no DB work left. This deviates from upstream, where the close self-joins.</item>
/// <item>From <see cref="OnBackgroundError"/>, <see cref="OnErrorRecoveryBegin"/> and the flush and compaction
/// callbacks, the DB must not be closed, and nothing may wait for background work or the write queue (a flush or
/// compaction with wait, <see cref="RocksDatabase.Resume"/>): the caller may be that work, so these deadlock.</item>
/// </list>
/// </para>
/// </summary>
public abstract class EventListener
{
    /// <summary>A flush job is about to write an L0 file (EventListener::OnFlushBegin).</summary>
    public virtual void OnFlushBegin(RocksDatabase? db, FlushJobInfo info)
    {
    }

    /// <summary>
    /// A manual flush was scheduled (EventListener::OnManualFlushScheduled): called by Flush, and by the operations that
    /// flush first (CompactRange, ingestion, checkpoints, GetLiveFiles, shutdown, ...), after the memtables were switched
    /// and before waiting for the flush; also when there was nothing to flush. One entry per column family; more than one
    /// only with atomic_flush.
    /// </summary>
    public virtual void OnManualFlushScheduled(RocksDatabase? db, IReadOnlyList<ManualFlushInfo> info)
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

    /// <summary>
    /// A compaction wrote its outputs and is about to commit them to the MANIFEST (EventListener::OnCompactionPreCommit):
    /// the input files are still marked as being compacted and the outputs are not yet visible. Called between
    /// <see cref="OnCompactionBegin"/> and <see cref="OnCompactionCompleted"/> of the same compaction, at most once, and
    /// only when <see cref="OnCompactionBegin"/> was. <see cref="CompactionJobInfo.Status"/> does not reflect the outcome of
    /// the commit. Skipped while the DB shuts down.
    /// </summary>
    public virtual void OnCompactionPreCommit(RocksDatabase? db, CompactionJobInfo info)
    {
    }

    /// <summary>A compaction finished, successfully or not (EventListener::OnCompactionCompleted).</summary>
    public virtual void OnCompactionCompleted(RocksDatabase? db, CompactionJobInfo info)
    {
    }

    /// <summary>
    /// A subcompaction is about to run (EventListener::OnSubcompactionBegin), on the thread that runs it. A compaction split
    /// into N key ranges raises one <see cref="OnCompactionBegin"/>, then N of these; one that is not split raises one (a
    /// compaction is always run as subcompactions). Not raised for trivial moves, FIFO deletions, trivial copies or
    /// compactions run by a <see cref="CompactionService"/> (the remote worker's listeners see them). Skipped while the DB
    /// shuts down and for a canceled manual compaction.
    /// </summary>
    public virtual void OnSubcompactionBegin(SubcompactionJobInfo info)
    {
    }

    /// <summary>
    /// A subcompaction finished, successfully or not (EventListener::OnSubcompactionCompleted); only raised when
    /// <see cref="OnSubcompactionBegin"/> was raised for it. Its output files were reported (OnTableFileCreated) before.
    /// </summary>
    public virtual void OnSubcompactionCompleted(SubcompactionJobInfo info)
    {
    }

    /// <summary>A flush or compaction finished writing a table file (EventListener::OnTableFileCreated).</summary>
    public virtual void OnTableFileCreated(TableFileCreationInfo info)
    {
    }

    /// <summary>
    /// A flush, compaction or recovery is about to create a table file (EventListener::OnTableFileCreationStarted);
    /// <see cref="OnTableFileCreated"/> follows once it is finished or failed.
    /// </summary>
    public virtual void OnTableFileCreationStarted(TableFileCreationBriefInfo info)
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

    /// <summary>
    /// Deprecated, use <see cref="OnErrorRecoveryEnd"/> (EventListener::OnErrorRecoveryCompleted): called right before
    /// <see cref="OnErrorRecoveryEnd"/> with the error that was recovered from (upstream calls it for every recovery end,
    /// also a failed one).
    /// </summary>
    [Obsolete("Use OnErrorRecoveryEnd (EventListener::OnErrorRecoveryCompleted is deprecated upstream).")]
    public virtual void OnErrorRecoveryCompleted(RocksDbException oldBackgroundError)
    {
    }

    /// <summary>A recovery (automatic or <see cref="RocksDatabase.Resume"/>) ended (EventListener::OnErrorRecoveryEnd).</summary>
    public virtual void OnErrorRecoveryEnd(BackgroundErrorRecoveryInfo info)
    {
    }

    /// <summary>
    /// A flush, compaction or recovery is about to create a blob file (EventListener::OnBlobFileCreationStarted);
    /// <see cref="OnBlobFileCreated"/> follows once it is finished or abandoned.
    /// </summary>
    public virtual void OnBlobFileCreationStarted(BlobFileCreationBriefInfo info)
    {
    }

    /// <summary>A blob file was finished, or abandoned after a failure (EventListener::OnBlobFileCreated).</summary>
    public virtual void OnBlobFileCreated(BlobFileCreationInfo info)
    {
    }

    /// <summary>An obsolete blob file was deleted (EventListener::OnBlobFileDeleted).</summary>
    public virtual void OnBlobFileDeleted(BlobFileDeletionInfo info)
    {
    }

    /// <summary>
    /// Whether the OnFile*Finish callbacks and <see cref="OnIOError"/> are called for this listener
    /// (EventListener::ShouldBeNotifiedOnFileIO). Read once when the DB is opened (upstream asks again for every file it
    /// opens). The default is false.
    /// </summary>
    public virtual bool ShouldBeNotifiedOnFileIO() => false;

    /// <summary>A read of a file finished (EventListener::OnFileReadFinish). See <see cref="ShouldBeNotifiedOnFileIO"/>.</summary>
    public virtual void OnFileReadFinish(in FileOperationInfo info)
    {
    }

    /// <summary>
    /// A write to a file finished (EventListener::OnFileWriteFinish): with the built-in <see cref="Env.LocalFileSystem"/>
    /// each write of the file's buffer to the OS, like upstream's WritableFileWriter; with other file systems each append.
    /// </summary>
    public virtual void OnFileWriteFinish(in FileOperationInfo info)
    {
    }

    /// <summary>A flush of a file's buffered data to the OS finished (EventListener::OnFileFlushFinish).</summary>
    public virtual void OnFileFlushFinish(in FileOperationInfo info)
    {
    }

    /// <summary>A sync (fdatasync / fsync) of a file finished (EventListener::OnFileSyncFinish).</summary>
    public virtual void OnFileSyncFinish(in FileOperationInfo info)
    {
    }

    /// <summary>A range sync of a file (bytes_per_sync / wal_bytes_per_sync) finished (EventListener::OnFileRangeSyncFinish).</summary>
    public virtual void OnFileRangeSyncFinish(in FileOperationInfo info)
    {
    }

    /// <summary>A truncation of a file finished (EventListener::OnFileTruncateFinish): a direct I/O file cut to its size on close.</summary>
    public virtual void OnFileTruncateFinish(in FileOperationInfo info)
    {
    }

    /// <summary>
    /// A close of a written file finished (EventListener::OnFileCloseFinish). As upstream's, its status is the one of the
    /// flush that precedes the close; a failure of the close itself is reported by <see cref="OnIOError"/>.
    /// </summary>
    public virtual void OnFileCloseFinish(in FileOperationInfo info)
    {
    }

    /// <summary>
    /// A file operation failed (EventListener::OnIOError): reads of table and blob files and the writes, flushes, syncs and
    /// closes of the files that report OnFile*Finish (not the reads of MANIFEST and WAL files, as upstream), for the
    /// listeners whose <see cref="ShouldBeNotifiedOnFileIO"/> returned true; and the MANIFEST checks on close
    /// (<see cref="FileOperationType.Verify"/>: a size mismatch or a corrupt record), which upstream reports to every
    /// listener.
    /// </summary>
    public virtual void OnIOError(IOErrorInfo info)
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
