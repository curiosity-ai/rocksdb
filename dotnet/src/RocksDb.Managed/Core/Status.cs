namespace RocksDb.Managed;

/// <summary>Mirrors rocksdb::Status::Code (include/rocksdb/status.h). Values are kept identical.</summary>
public enum StatusCode : byte
{
    Ok = 0,
    NotFound = 1,
    Corruption = 2,
    NotSupported = 3,
    InvalidArgument = 4,
    IOError = 5,
    MergeInProgress = 6,
    Incomplete = 7,
    ShutdownInProgress = 8,
    TimedOut = 9,
    Aborted = 10,
    Busy = 11,
    Expired = 12,
    TryAgain = 13,
    CompactionTooLarge = 14,
    ColumnFamilyDropped = 15,
}

/// <summary>Mirrors rocksdb::Status::SubCode.</summary>
public enum StatusSubCode : byte
{
    None = 0,
    MutexTimeout = 1,
    LockTimeout = 2,
    LockLimit = 3,
    NoSpace = 4,
    Deadlock = 5,
    StaleFile = 6,
    MemoryLimit = 7,
    SpaceLimit = 8,
    PathNotFound = 9,
    MergeOperandsInsufficientCapacity = 10,
    ManualCompactionPaused = 11,
    Overwritten = 12,
    TxnNotPrepared = 13,
    IOFenced = 14,
    MergeOperatorFailed = 15,
    MergeOperandThresholdExceeded = 16,
    PrefetchLimitReached = 17,
    NotExpectedCodePath = 18,
    CompactionAborted = 19,
}

/// <summary>Mirrors rocksdb::Status::Severity (include/rocksdb/status.h). Values are kept identical.</summary>
public enum ErrorSeverity : byte
{
    NoError = 0,
    SoftError = 1,
    HardError = 2,
    FatalError = 3,
    UnrecoverableError = 4,
}

/// <summary>Mirrors rocksdb::IOStatus::IOErrorScope (include/rocksdb/io_status.h). Values are kept identical.</summary>
public enum IOErrorScope : byte
{
    FileSystem = 0,
    File = 1,
    Range = 2,
}

/// <summary>
/// Error surfaced by the managed port. The C++ code returns Status values; in the port, failures are
/// exceptions on the public API, while hot internal paths use bool/Try-style returns and only throw
/// on genuinely exceptional conditions (corruption, I/O errors).
/// </summary>
public class RocksDbException : Exception
{
    public StatusCode Code { get; }
    public StatusSubCode SubCode { get; }

    public RocksDbException(StatusCode code, string message, StatusSubCode subCode = StatusSubCode.None, Exception? inner = null)
        : base(message, inner)
    {
        Code = code;
        SubCode = subCode;
    }

    /// <summary>
    /// IOStatus::GetRetryable: the failed I/O may succeed when retried (e.g. a transient remote storage error). Background
    /// errors with this flag are recovered automatically (see <see cref="DbOptions.MaxBgErrorResumeCount"/>).
    /// </summary>
    public bool Retryable { get; init; }

    /// <summary>IOStatus::GetDataLoss: data was lost (a background error with this flag is unrecoverable).</summary>
    public bool DataLoss { get; init; }

    /// <summary>IOStatus::GetScope. A file-scoped I/O error is treated as retryable by the write path.</summary>
    public IOErrorScope Scope { get; init; }

    /// <summary>
    /// Status::severity: set on background errors reported by the DB (<see cref="RocksDatabase.BackgroundError"/>,
    /// failed writes after a background error). <see cref="ErrorSeverity.NoError"/> otherwise.
    /// </summary>
    public ErrorSeverity Severity { get; init; }

    /// <summary>A new exception object for the same status (flags and severity kept), with this one as inner exception.</summary>
    internal RocksDbException Rethrowable() =>
        new(Code, Message, SubCode, this) { Retryable = Retryable, DataLoss = DataLoss, Scope = Scope, Severity = Severity };

    /// <summary>Copy of this error with the given severity (Status(const Status&amp;, Severity)).</summary>
    public RocksDbException WithSeverity(ErrorSeverity severity) =>
        new(Code, Message, SubCode, InnerException ?? this) { Retryable = Retryable, DataLoss = DataLoss, Scope = Scope, Severity = severity };

    public static RocksDbException Corruption(string message) => new(StatusCode.Corruption, "Corruption: " + message);
    public static RocksDbException InvalidArgument(string message) => new(StatusCode.InvalidArgument, "Invalid argument: " + message);
    public static RocksDbException NotSupported(string message) => new(StatusCode.NotSupported, "Not implemented: " + message);
    public static RocksDbException IOError(string message, Exception? inner = null) => new(StatusCode.IOError, "IO error: " + message, StatusSubCode.None, inner);

    /// <summary>An IOError with the retryable flag (IOStatus::IOError + SetRetryable(true)).</summary>
    public static RocksDbException RetryableIOError(string message, Exception? inner = null) =>
        new(StatusCode.IOError, "IO error: " + message, StatusSubCode.None, inner) { Retryable = true };

    /// <summary>IOStatus::NoSpace: the device is out of space.</summary>
    public static RocksDbException NoSpace(string message, Exception? inner = null) =>
        new(StatusCode.IOError, "IO error: No space left on device: " + message, StatusSubCode.NoSpace, inner);

    /// <summary>Status::SpaceLimit: a configured space limit was reached.</summary>
    public static RocksDbException SpaceLimit(string message) =>
        new(StatusCode.IOError, "IO error: Space limit reached: " + message, StatusSubCode.SpaceLimit);

    // errno / HRESULT values of "no space left on device" (ENOSPC on Linux/macOS, EDQUOT, ERROR_DISK_FULL,
    // ERROR_HANDLE_DISK_FULL).
    private const int EnospcLinux = 28;
    private const int EdquotLinux = 122;
    private const int EdquotDarwin = 69;
    private const int ErrorDiskFull = unchecked((int)0x80070070);
    private const int ErrorHandleDiskFull = unchecked((int)0x80070027);

    /// <summary>
    /// Maps an exception from a file system or a background job to a status (the IOStatus a posix Env would return):
    /// <see cref="RocksDbException"/> is returned as is, an out-of-space <see cref="IOException"/> becomes
    /// <see cref="NoSpace"/>, other I/O and access errors become plain IOErrors.
    /// </summary>
    public static RocksDbException FromException(Exception e)
    {
        ArgumentNullException.ThrowIfNull(e);
        if (e is RocksDbException r) return r;
        if (e is AggregateException { InnerExceptions.Count: 1 } a) return FromException(a.InnerExceptions[0]);
        if (e is IOException io)
        {
            int h = io.HResult;
            if (h is EnospcLinux or EdquotLinux or ErrorDiskFull or ErrorHandleDiskFull || (h == EdquotDarwin && OperatingSystem.IsMacOS()))
            {
                return NoSpace(io.Message, io);
            }
        }
        return IOError(e.Message, e);
    }
}

/// <summary>Throw helpers kept out of line so callers stay small and inlinable.</summary>
internal static class ThrowHelper
{
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    public static void Corruption(string message) => throw RocksDbException.Corruption(message);

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    public static void InvalidArgument(string message) => throw RocksDbException.InvalidArgument(message);

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    public static void NotSupported(string message) => throw RocksDbException.NotSupported(message);

    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    public static T Corruption<T>(string message) => throw RocksDbException.Corruption(message);
}
