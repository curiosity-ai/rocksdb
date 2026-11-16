using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// Consumes the result of a <c>synchronous: true</c> call. Not a port of RocksDB code: managed infrastructure for the
/// one-implementation-for-sync-and-async convention (dotnet/CLAUDE.md).
/// <para>
/// Invariant: with <c>synchronous: true</c> every wait inside the callee branches on the flag, so the returned
/// <see cref="ValueTask"/> is already completed. The callees are built with pooling builders
/// (<see cref="PoolingAsyncValueTaskMethodBuilder"/>, <see cref="OwnedValueTaskMethodBuilder{TResult}"/>), and calling
/// <c>GetResult</c> on an incomplete pooled <see cref="System.Threading.Tasks.Sources.IValueTaskSource"/> is undefined
/// behaviour (it can return a recycled result or corrupt the pool). If the invariant ever breaks, debug builds assert
/// and release builds block on <see cref="ValueTask.AsTask"/>, which is valid for an incomplete pooled task and
/// consumes it exactly once. The completed path is an inlined status check plus the ordinary <c>GetResult</c>: no
/// allocation, no extra virtual call for a task that completed synchronously (its source is null).
/// </para>
/// </summary>
internal static class SyncValueTask
{
    /// <summary>Returns the result of <paramref name="task"/>, which a <c>synchronous: true</c> call returned.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GetResult(ValueTask task)
    {
        if (task.IsCompleted) task.GetAwaiter().GetResult();
        else BlockOnIncomplete(task);
    }

    /// <summary>Returns the result of <paramref name="task"/>, which a <c>synchronous: true</c> call returned.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T GetResult<T>(ValueTask<T> task) => task.IsCompleted ? task.GetAwaiter().GetResult() : BlockOnIncomplete(task);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void BlockOnIncomplete(ValueTask task)
    {
        Debug.Fail("A synchronous: true call returned an incomplete ValueTask.");
        task.AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static T BlockOnIncomplete<T>(ValueTask<T> task)
    {
        Debug.Fail("A synchronous: true call returned an incomplete ValueTask.");
        return task.AsTask().GetAwaiter().GetResult();
    }
}
