using RocksDb.Managed.Db;
using RocksDb.Managed.Table;

namespace RocksDb.Managed;

/// <summary>Port of rocksdb::CompactionServiceJobStatus (include/rocksdb/options.h). Values are kept identical (C API ints).</summary>
public enum CompactionServiceJobStatus : byte
{
    /// <summary>The job was scheduled / finished; its result is installed.</summary>
    Success = 0,
    /// <summary>The job failed; the compaction fails (Incomplete, or the worker's status).</summary>
    Failure = 1,
    /// <summary>The job was aborted; the compaction fails with Aborted.</summary>
    Aborted = 2,
    /// <summary>Run this compaction locally instead.</summary>
    UseLocal = 3,
}

/// <summary>Port of Env::Priority (include/rocksdb/env.h): the thread pool a compaction runs in. Values are kept identical.</summary>
public enum CompactionThreadPriority : byte
{
    Bottom = 0,
    Low = 1,
    High = 2,
    User = 3,
}

/// <summary>
/// Describes a compaction handed to a <see cref="CompactionService"/>. Port of rocksdb::CompactionServiceJobInfo
/// (include/rocksdb/options.h).
/// </summary>
/// <param name="DbName">Path of the primary DB (the <c>dbPath</c> a worker passes to <see cref="RocksDatabase.OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/>).</param>
/// <param name="DbId">The DB's identity.</param>
/// <param name="DbSessionId">The primary's session id; with <paramref name="DbId"/> and <paramref name="JobId"/> it identifies the job across DBs and sessions.</param>
/// <param name="ColumnFamilyId">Id of the compacted column family.</param>
/// <param name="ColumnFamilyName">Name of the compacted column family.</param>
/// <param name="JobId">The compaction's job id (unique within the DB session).</param>
/// <param name="Priority">Thread pool of the compaction.</param>
/// <param name="CompactionReason">Why the compaction was picked.</param>
/// <param name="IsFullCompaction">Every file of the column family is an input.</param>
/// <param name="IsManualCompaction">CompactRange / CompactFiles.</param>
/// <param name="BottommostLevel">The output is the bottommost level of the data.</param>
/// <param name="BaseInputLevel">Smallest input level.</param>
/// <param name="OutputLevel">Output level.</param>
public sealed record CompactionServiceJobInfo(string DbName, string DbId, string DbSessionId, uint ColumnFamilyId, string ColumnFamilyName, ulong JobId,
    CompactionThreadPriority Priority, CompactionReason CompactionReason, bool IsFullCompaction, bool IsManualCompaction, bool BottommostLevel,
    int BaseInputLevel, int OutputLevel);

/// <summary>Answer of <see cref="CompactionService.ScheduleAsync"/> (rocksdb::CompactionServiceScheduleResponse).</summary>
/// <param name="ScheduledJobId">An id of the scheduled job, unique across DBs and sessions, passed back to <see cref="CompactionService.WaitAsync"/>.</param>
/// <param name="Status"><see cref="CompactionServiceJobStatus.Success"/> once scheduled; other values make the compaction run locally (UseLocal) or fail.</param>
public readonly record struct CompactionServiceScheduleResponse(string ScheduledJobId, CompactionServiceJobStatus Status);

/// <summary>Answer of <see cref="CompactionService.WaitAsync"/>: the job status and the worker's serialized result.</summary>
/// <param name="Status">Status of the job.</param>
/// <param name="Result">The bytes returned by the worker's OpenAndCompact (also on failure, when the worker produced one); null if none.</param>
public readonly record struct CompactionServiceWaitResult(CompactionServiceJobStatus Status, byte[]? Result);

/// <summary>
/// Runs compactions outside the DB process ("remote compaction"). Port of rocksdb::CompactionService
/// (include/rocksdb/options.h); set it with <see cref="DbOptions.CompactionService"/>.
/// <para>
/// For every compaction that rewrites data the primary DB calls <see cref="ScheduleAsync"/> with the serialized job
/// (CompactionServiceInput, byte-compatible with native RocksDB), then <see cref="WaitAsync"/>. The service runs the job
/// with <see cref="RocksDatabase.OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/>
/// (or native <c>DB::OpenAndCompact</c>) on a host that can read the DB directory, and returns the worker's serialized
/// result; the primary then moves the output files into the DB and installs them. The default implementation runs every
/// compaction locally. Calls come from the compaction threads (one job per call, possibly concurrently).
/// </para>
/// </summary>
public abstract class CompactionService
{
    /// <summary>Name of the service (Customizable::Name).</summary>
    public abstract string Name { get; }

    /// <summary>Schedules the compaction; the default asks the DB to compact locally.</summary>
    public virtual ValueTask<CompactionServiceScheduleResponse> ScheduleAsync(CompactionServiceJobInfo info, ReadOnlyMemory<byte> compactionServiceInput,
        CancellationToken cancellationToken) =>
        ValueTask.FromResult(new CompactionServiceScheduleResponse("", CompactionServiceJobStatus.UseLocal));

    /// <summary>Waits for a scheduled job and returns its status and the worker's serialized result; the default runs it locally.</summary>
    public virtual ValueTask<CompactionServiceWaitResult> WaitAsync(string scheduledJobId, CancellationToken cancellationToken) =>
        ValueTask.FromResult(new CompactionServiceWaitResult(CompactionServiceJobStatus.UseLocal, null));

    /// <summary>Cancels the jobs being waited for (called when the DB shuts down background work).</summary>
    public virtual void CancelAwaitingJobs()
    {
    }

    /// <summary>Called after the primary tried to install a job's result (Success) or declined / failed it.</summary>
    public virtual void OnInstallation(string scheduledJobId, CompactionServiceJobStatus status)
    {
    }
}

/// <summary>Options of <see cref="RocksDatabase.OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/> (rocksdb::OpenAndCompactOptions).</summary>
public sealed class OpenAndCompactOptions
{
    /// <summary>Cancels the compaction (OpenAndCompactOptions::canceled): it then fails with Incomplete / ManualCompactionPaused.</summary>
    public CancellationToken Canceled { get; init; }

    /// <summary>
    /// Retries of the secondary open when the primary replaced CURRENT / the MANIFEST concurrently
    /// (max_secondary_open_retries, default 2).
    /// </summary>
    public uint MaxSecondaryOpenRetries { get; init; } = 2;
}

/// <summary>
/// The objects a remote worker needs that the OPTIONS file can't carry: comparator, merge operator, compaction filter,
/// ... (rocksdb::CompactionServiceOptionsOverride). They must match the primary's. Plain options can be overridden
/// with <see cref="OptionsMap"/> ("name" → OPTIONS-file value, applied to the DB and column family options).
/// </summary>
public sealed class CompactionServiceOptionsOverride
{
    /// <summary>File system holding the DB (default: the local file system).</summary>
    public Env.IFileSystem? FileSystem { get; init; }
    /// <summary>The column family's comparator (default: bytewise).</summary>
    public Comparator Comparator { get; init; } = Comparator.Bytewise;
    public MergeOperator? MergeOperator { get; init; }
    public CompactionFilter? CompactionFilter { get; init; }
    public CompactionFilterFactory? CompactionFilterFactory { get; init; }
    public SliceTransform? PrefixExtractor { get; init; }
    public SstPartitionerFactory? SstPartitionerFactory { get; init; }
    /// <summary>Table properties collectors of the outputs (table_properties_collector_factories).</summary>
    public IReadOnlyList<TablePropertiesCollectorFactory>? TablePropertiesCollectorFactories { get; init; }
    public FileChecksumGenFactory? FileChecksumGenFactory { get; init; }
    public Statistics? Statistics { get; init; }
    public InfoLogger? InfoLog { get; init; }
    public IReadOnlyList<EventListener>? Listeners { get; init; }
    /// <summary>Option overrides by OPTIONS-file name (options_map), e.g. "compression" → "kZSTD".</summary>
    public IReadOnlyDictionary<string, string>? OptionsMap { get; init; }
}
