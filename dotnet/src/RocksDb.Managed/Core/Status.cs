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

    public static RocksDbException Corruption(string message) => new(StatusCode.Corruption, "Corruption: " + message);
    public static RocksDbException InvalidArgument(string message) => new(StatusCode.InvalidArgument, "Invalid argument: " + message);
    public static RocksDbException NotSupported(string message) => new(StatusCode.NotSupported, "Not implemented: " + message);
    public static RocksDbException IOError(string message, Exception? inner = null) => new(StatusCode.IOError, "IO error: " + message, StatusSubCode.None, inner);
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
