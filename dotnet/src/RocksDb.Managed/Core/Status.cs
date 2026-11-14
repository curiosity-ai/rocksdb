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

    /// <summary>
    /// The operation never reached the file: its writer had already failed ("Writer has previous error.";
    /// WritableFileWriter's failed_with_previous_error). The status says nothing about the original failure (its scope,
    /// retryable flag or subcode are lost), so it must not be classified as a new background error. Not carried over by
    /// <see cref="Rethrowable"/> / <see cref="WithSeverity"/>.
    /// </summary>
    internal bool WriterHasPreviousError { get; init; }

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

    /// <summary>IOStatus::PathNotFound: the file or directory does not exist.</summary>
    public static RocksDbException PathNotFound(string message, Exception? inner = null) =>
        new(StatusCode.IOError, "IO error: No such file or directory: " + message, StatusSubCode.PathNotFound, inner);

    // Error numbers mapped by the native environments. Unix (errno): ENOENT and ENOSPC have the same value on Linux and
    // macOS, ESTALE and EDQUOT differ. Windows: Win32 error codes (winerror.h).
    private const int ENOENT = 2;
    private const int ENOSPC = 28;
    private const int ESTALE_LINUX = 116;
    private const int ESTALE_DARWIN = 70;
    private const int EDQUOT_LINUX = 122;
    private const int EDQUOT_DARWIN = 69;
    private const int ERROR_FILE_NOT_FOUND = 2;
    private const int ERROR_PATH_NOT_FOUND = 3;
    private const int ERROR_HANDLE_DISK_FULL = 39;
    private const int ERROR_DISK_FULL = 112;
    // An IOException raised for a Win32 error carries HRESULT_FROM_WIN32(error) = 0x8007xxxx as its HResult.
    private const int FACILITY_WIN32_MASK = unchecked((int)0xFFFF0000);
    private const int FACILITY_WIN32 = unchecked((int)0x80070000);

    private static bool IsEstale(int errno) => errno == (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ? ESTALE_DARWIN : ESTALE_LINUX);

    // EDQUOT is not mapped by io_posix.cc (it becomes a plain IOError there). A quota is exhausted the way a device is
    // filled, and is cleared the same way (by freeing space), so it gets the out-of-space handling as well.
    private static bool IsEdquot(int errno) => errno == (OperatingSystem.IsMacOS() || OperatingSystem.IsIOS() ? EDQUOT_DARWIN : EDQUOT_LINUX);

    private static string Join(string? context, string? path, string? detail)
    {
        string m = context ?? "";
        if (!string.IsNullOrEmpty(path)) m = m.Length == 0 ? path : m + ": " + path; // IOErrorMsg
        if (!string.IsNullOrEmpty(detail)) m = m.Length == 0 ? detail : m + ": " + detail;
        return m;
    }

    /// <summary>
    /// Port of IOError(context, file_name, err_number) of env/io_posix.cc:69-87: ENOSPC becomes a retryable
    /// <see cref="NoSpace"/>, ESTALE an IOError with <see cref="StatusSubCode.StaleFile"/>, ENOENT
    /// <see cref="PathNotFound"/>, anything else a plain IOError. <paramref name="detail"/> is errnoStr(err_number).
    /// </summary>
    internal static RocksDbException FromErrno(string? context, string? path, int errno, string? detail, Exception? inner = null)
    {
        string m = Join(context, path, detail);
        if (errno == ENOSPC || IsEdquot(errno))
        {
            return new(StatusCode.IOError, "IO error: No space left on device: " + m, StatusSubCode.NoSpace, inner) { Retryable = true };
        }
        if (IsEstale(errno)) return new(StatusCode.IOError, "IO error: Stale file handle: " + m, StatusSubCode.StaleFile, inner);
        if (errno == ENOENT) return PathNotFound(m, inner);
        return IOError(m, inner);
    }

    /// <summary>
    /// Port of port::IOErrorFromWindowsError (port/win/io_win.h): ERROR_HANDLE_DISK_FULL / ERROR_DISK_FULL become
    /// <see cref="NoSpace"/> (not retryable, unlike the posix mapping), ERROR_FILE_NOT_FOUND / ERROR_PATH_NOT_FOUND
    /// <see cref="PathNotFound"/>, anything else a plain IOError.
    /// </summary>
    internal static RocksDbException FromWin32Error(string? context, string? path, int error, string? detail, Exception? inner = null)
    {
        string m = Join(context, path, detail);
        if (error is ERROR_HANDLE_DISK_FULL or ERROR_DISK_FULL) return NoSpace(m, inner);
        if (error is ERROR_FILE_NOT_FOUND or ERROR_PATH_NOT_FOUND) return PathNotFound(m, inner);
        return IOError(m, inner);
    }

    /// <summary>
    /// Maps an exception thrown by the .NET file APIs to the IOStatus the native environment returns for the same
    /// failure (<see cref="FromErrno"/> on Unix, where .NET carries the errno as the exception's HResult;
    /// <see cref="FromWin32Error"/> on Windows, where it carries HRESULT_FROM_WIN32). A <see cref="RocksDbException"/> is
    /// returned unchanged. Every file system error path goes through here, so the subcode (and with it the background
    /// error severity and the out-of-space recovery) does not depend on which API failed.
    /// </summary>
    internal static RocksDbException FromIOException(string? context, string? path, Exception e)
    {
        if (e is RocksDbException r) return r;
        if (e is AggregateException { InnerExceptions.Count: 1 } a) return FromIOException(context, path, a.InnerExceptions[0]);
        string detail = e.Message;
        if (e is FileNotFoundException or DirectoryNotFoundException) return PathNotFound(Join(context, path, detail), e);
        if (e is IOException io)
        {
            int h = io.HResult;
            if (OperatingSystem.IsWindows())
            {
                if ((h & FACILITY_WIN32_MASK) == FACILITY_WIN32) return FromWin32Error(context, path, h & 0xFFFF, detail, e);
            }
            else if (h > 0)
            {
                return FromErrno(context, path, h, detail, e);
            }
        }
        return IOError(Join(context, path, detail), e);
    }

    /// <summary>
    /// Maps an exception from a file system or a background job to a status (the IOStatus a native Env would return,
    /// see <see cref="FromIOException"/>): <see cref="RocksDbException"/> is returned as is, an out-of-space
    /// <see cref="IOException"/> becomes <see cref="NoSpace"/>, other I/O and access errors become IOErrors.
    /// </summary>
    public static RocksDbException FromException(Exception e)
    {
        ArgumentNullException.ThrowIfNull(e);
        return FromIOException(null, null, e);
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
