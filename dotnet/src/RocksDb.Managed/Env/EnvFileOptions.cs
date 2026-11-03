namespace RocksDb.Managed.Env;

/// <summary>I/O priority classes. Values mirror rocksdb::Env::IOPriority (include/rocksdb/env.h).</summary>
public enum IoPriority : byte
{
    Low = 0,
    Mid = 1,
    High = 2,
    User = 3,
    Total = 4,
}

/// <summary>The kind of I/O a rate limiter request is for (RateLimiter::OpType, include/rocksdb/rate_limiter.h).</summary>
public enum RateLimiterOpType
{
    Read = 0,
    Write = 1,
}

/// <summary>
/// Rate limiter hook (interface subset of include/rocksdb/rate_limiter.h). <see cref="LocalFileSystem"/> writers
/// request tokens before every physical write, in chunks of at most <see cref="SingleBurstBytes"/>, exactly like
/// WritableFileWriter::WriteBuffered; table readers charge block reads made with a rate limiter priority (compaction
/// inputs, <see cref="ReadOptions.RateLimiterPriority"/>) like RandomAccessFileReader::ChargeRateLimiterForRead.
/// Implementations must be thread-safe. <see cref="GenericRateLimiter"/> is the built-in implementation.
/// </summary>
public interface IRateLimiter
{
    /// <summary>Maximum bytes that can be granted in a single request (GetSingleBurstBytes).</summary>
    long SingleBurstBytes { get; }

    /// <summary>Blocks until <paramref name="bytes"/> (≤ <see cref="SingleBurstBytes"/>) may be written.</summary>
    void Request(long bytes, IoPriority priority);

    /// <summary>Async variant of <see cref="Request"/>. Should complete synchronously when tokens are available.</summary>
    ValueTask RequestAsync(long bytes, IoPriority priority, CancellationToken cancellationToken);

    /// <summary>
    /// IsRateLimited(op_type): whether requests of this kind are charged. The default charges writes only (the
    /// behaviour of implementations written before reads were charged).
    /// </summary>
    bool IsRateLimited(RateLimiterOpType opType) => opType == RateLimiterOpType.Write;

    /// <summary>
    /// Request(bytes, pri, stats, op_type): charges <paramref name="bytes"/> when <see cref="IsRateLimited"/> and records
    /// the RATE_LIMITER_* statistics in <paramref name="statistics"/> (may be null). The default forwards to
    /// <see cref="Request(long, IoPriority)"/> without statistics.
    /// </summary>
    void Request(long bytes, IoPriority priority, RocksDb.Managed.Statistics? statistics, RateLimiterOpType opType)
    {
        if (IsRateLimited(opType)) Request(bytes, priority);
    }

    /// <summary>Async variant of <see cref="Request(long, IoPriority, RocksDb.Managed.Statistics?, RateLimiterOpType)"/>.</summary>
    ValueTask RequestAsync(long bytes, IoPriority priority, RocksDb.Managed.Statistics? statistics, RateLimiterOpType opType, CancellationToken cancellationToken) =>
        IsRateLimited(opType) ? RequestAsync(bytes, priority, cancellationToken) : ValueTask.CompletedTask;
}

/// <summary>
/// Options for writable files (subset of rocksdb::FileOptions/EnvOptions that affects WritableFileWriter and
/// PosixWritableFile). Instances are immutable after construction and may be shared.
/// </summary>
public sealed class WritableFileOptions
{
    public static readonly WritableFileOptions Default = new();

    /// <summary>Initial size of the pooled write buffer (WritableFileWriter starts at min(65536, max)).</summary>
    public int InitialBufferSize { get; init; } = 64 * 1024;

    /// <summary>Upper bound the write buffer may grow to (DBOptions::writable_file_max_buffer_size, default 1 MiB).</summary>
    public int MaxBufferSize { get; init; } = 1024 * 1024;

    /// <summary>
    /// When &gt; 0, space is reserved ahead of the write position in chunks of this size using
    /// fallocate(FALLOC_FL_KEEP_SIZE) (PosixWritableFile::PrepareWrite). Linux only; ignored elsewhere.
    /// </summary>
    public long PreallocationBlockSize { get; init; }

    /// <summary>
    /// When &gt; 0, written data is incrementally pushed to the device every this many bytes using
    /// sync_file_range(SYNC_FILE_RANGE_WRITE) (DBOptions::bytes_per_sync). Linux only.
    /// </summary>
    public long BytesPerSync { get; init; }

    /// <summary>
    /// Bypass the OS page cache (O_DIRECT). Writes are issued in whole <see cref="DirectIOAlignment"/> pages from an
    /// aligned native buffer and the file is truncated to its logical size on close (WritableFileWriter::WriteDirect).
    /// Linux only; opening fails with NotSupported elsewhere or on file systems without O_DIRECT (e.g. tmpfs).
    /// </summary>
    public bool UseDirectIO { get; init; }

    /// <summary>Alignment (logical block size) used for direct I/O. Default 4096.</summary>
    public int DirectIOAlignment { get; init; } = 4096;

    /// <summary>Optional rate limiter applied to physical writes.</summary>
    public IRateLimiter? RateLimiter { get; init; }

    /// <summary>Priority passed to <see cref="RateLimiter"/>.</summary>
    public IoPriority IoPriority { get; init; } = IoPriority.Total;

    /// <summary>
    /// The DB's file_checksum_gen_factory for this file (WritableFileWriter::checksum_generator_). Applied by the DB's
    /// table and blob writers (FileChecksumHelper.CreateWritable), not by the file system.
    /// </summary>
    public FileChecksumGenFactory? FileChecksumGenFactory { get; init; }

    /// <summary>Statistics receiving the RATE_LIMITER_* write tickers of <see cref="RateLimiter"/> requests (WritableFileWriter::stats_).</summary>
    public RocksDb.Managed.Statistics? Statistics { get; init; }
}

/// <summary>Options for random-access readers (RandomAccessFileReader / PosixRandomAccessFile).</summary>
public sealed class RandomAccessFileOptions
{
    public static readonly RandomAccessFileOptions Default = new();

    /// <summary>
    /// Bypass the OS page cache (O_DIRECT). Reads are internally widened to aligned offsets/lengths through a
    /// pooled aligned bounce buffer, so callers can use any buffer. Linux only.
    /// </summary>
    public bool UseDirectIO { get; init; }

    /// <summary>Alignment used for direct I/O. Default 4096.</summary>
    public int DirectIOAlignment { get; init; } = 4096;
}

/// <summary>Options for sequential readers (SequentialFileReader).</summary>
public sealed class SequentialFileOptions
{
    public static readonly SequentialFileOptions Default = new();

    /// <summary>
    /// When &gt; 0, reads are served from an internal pooled buffer refilled this many bytes at a time
    /// (ReadaheadSequentialFile in file/sequence_file_reader.cc). Useful for small reads; WAL readers that already
    /// read 32 KiB blocks do not need it.
    /// </summary>
    public int ReadaheadSize { get; init; }
}
