using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>Remote compaction worker entry point (see <see cref="CompactionService"/>).</summary>
public sealed partial class RocksDatabase
{
    /// <summary>
    /// Runs a remote compaction job (DB::OpenAndCompact): <paramref name="input"/> is the serialized job a primary passed
    /// to <see cref="CompactionService.ScheduleAsync"/> (managed or native RocksDB). The DB at <paramref name="dbPath"/> is
    /// opened as a secondary instance (its WALs are ignored) with the OPTIONS file the job names plus
    /// <paramref name="overrideOptions"/>; the output tables are written to <paramref name="outputDirectory"/> (relative
    /// to <paramref name="dbPath"/> unless it starts with it, like upstream's RemoteCompactionJobDir). Returns the
    /// serialized result to hand back to the primary from <see cref="CompactionService.WaitAsync"/>. A failed
    /// compaction throws <see cref="OpenAndCompactException"/>, which carries the serialized failure result as well.
    /// </summary>
    public static byte[] OpenAndCompact(OpenAndCompactOptions options, string dbPath, string outputDirectory, ReadOnlySpan<byte> input,
        CompactionServiceOptionsOverride overrideOptions)
    {
        ValueTask<byte[]> t = DbImpl.OpenAndCompactAsync(options, dbPath, outputDirectory, input.ToArray(), overrideOptions, synchronous: true,
            CancellationToken.None);
        return t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult();
    }

    /// <summary><see cref="OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/> with default options.</summary>
    public static byte[] OpenAndCompact(string dbPath, string outputDirectory, ReadOnlySpan<byte> input, CompactionServiceOptionsOverride overrideOptions) =>
        OpenAndCompact(new OpenAndCompactOptions(), dbPath, outputDirectory, input, overrideOptions);

    /// <summary>Async <see cref="OpenAndCompact(OpenAndCompactOptions, string, string, ReadOnlySpan{byte}, CompactionServiceOptionsOverride)"/>.</summary>
    public static ValueTask<byte[]> OpenAndCompactAsync(OpenAndCompactOptions options, string dbPath, string outputDirectory, ReadOnlyMemory<byte> input,
        CompactionServiceOptionsOverride overrideOptions, CancellationToken cancellationToken = default) =>
        DbImpl.OpenAndCompactAsync(options, dbPath, outputDirectory, input, overrideOptions, synchronous: false, cancellationToken);

    /// <summary>Number of compactions currently waiting for their remote worker (rocksdb.num-running-remote-compactions).</summary>
    public int NumRunningRemoteCompactions => _db.NumRunningRemoteCompactions;
}
