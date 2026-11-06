using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Table;

/// <summary>
/// ReadOptions::deadline for table file reads. Port of the deadline part of PrepareIOFromReadOptions
/// (file/file_util.h): a read that needs file I/O once the deadline has passed fails with TimedOut, while reads served
/// by the memtables or the block cache still succeed. The deadline of the running operation is kept per thread: the
/// DB sets it around Get / MultiGet and iterator moves, and block reads from the file check it.
/// </summary>
/// <remarks>
/// Deviations: the check covers the block reads that start on the thread running the operation (sync APIs, and the
/// synchronous part of the async ones); continuations of async reads on other threads are not checked.
/// ReadOptions::io_timeout is accepted like upstream's PosixFileSystem, which does not enforce IOOptions::timeout.
/// </remarks>
internal static class ReadDeadline
{
    [ThreadStatic]
    private static long t_deadlineMicros; // absolute, microseconds since the Unix epoch (SystemClock::NowMicros); 0 = none

    /// <summary>
    /// Makes <paramref name="options"/>' deadline the current thread's; returns the token for <see cref="Exit"/>
    /// (-1 when the options carry no deadline and nothing changed).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Enter(ReadOptions? options)
    {
        if (options is null || options.Deadline <= TimeSpan.Zero) return -1;
        return EnterSlow(options.Deadline);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long EnterSlow(TimeSpan deadline)
    {
        long prev = t_deadlineMicros;
        t_deadlineMicros = deadline.Ticks / TimeSpan.TicksPerMicrosecond;
        return prev;
    }

    /// <summary>Restores the deadline that was current before <see cref="Enter"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Exit(long token)
    {
        if (token >= 0) t_deadlineMicros = token;
    }

    /// <summary>Whether the current operation's deadline (if any) has passed.</summary>
    public static bool IsExceeded(ReadOptions? options) =>
        options is not null && options.Deadline > TimeSpan.Zero && NowMicros() > options.Deadline.Ticks / TimeSpan.TicksPerMicrosecond;

    /// <summary>PrepareIOFromReadOptions: TimedOut when the file is about to be read after the deadline.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CheckBeforeIo()
    {
        long d = t_deadlineMicros;
        if (d != 0 && NowMicros() > d) ThrowTimedOut();
    }

    private static long NowMicros() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond;

    [MethodImpl(MethodImplOptions.NoInlining)]
    [System.Diagnostics.CodeAnalysis.DoesNotReturn]
    internal static void ThrowTimedOut() => throw new RocksDbException(StatusCode.TimedOut, "Operation timed out: ");
}
