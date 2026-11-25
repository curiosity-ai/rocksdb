using System.Runtime.CompilerServices;

namespace RocksDb.Managed.Util;

/// <summary>
/// <see cref="Interlocked.Or(ref long, long)"/> / <see cref="Interlocked.And(ref long, long)"/> whose previous value is
/// used, written as compare-exchange loops. Not a port of RocksDB code (std::atomic fetch_or / fetch_and upstream).
/// </summary>
/// <remarks>
/// Works around a JIT bug of .NET 11.0.0 on x64: the minimal-optimization (Tier0) expansion of an
/// <c>Interlocked.Or</c> / <c>And</c> whose result is used can give the target address and the new value one register
/// (<c>mov rcx, rax; or rcx, rdx; lock cmpxchg [rcx], rcx</c>), so it writes through a garbage address and fails with a
/// NullReferenceException at the call. Every method runs at Tier0 first, so in a Release build on .NET 11 the first
/// inserts into a HyperClock cache failed. Optimized code and .NET 10 are not affected. Optimized code expands a used
/// result to this same loop, so nothing is lost; an <c>Interlocked.Or</c> / <c>And</c> whose result is discarded is a
/// single <c>lock or</c> / <c>lock and</c> and stays as it is.
/// </remarks>
internal static class AtomicBits
{
    /// <summary>Atomically ORs <paramref name="value"/> into <paramref name="location"/>; returns the previous value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Or(ref long location, long value)
    {
        long current = Volatile.Read(ref location);
        while (true)
        {
            long seen = Interlocked.CompareExchange(ref location, current | value, current);
            if (seen == current) return current;
            current = seen;
        }
    }

    /// <summary>Atomically ANDs <paramref name="value"/> into <paramref name="location"/>; returns the previous value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long And(ref long location, long value)
    {
        long current = Volatile.Read(ref location);
        while (true)
        {
            long seen = Interlocked.CompareExchange(ref location, current & value, current);
            if (seen == current) return current;
            current = seen;
        }
    }

    /// <summary>Atomically ORs <paramref name="value"/> into <paramref name="location"/>; returns the previous value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Or(ref int location, int value)
    {
        int current = Volatile.Read(ref location);
        while (true)
        {
            int seen = Interlocked.CompareExchange(ref location, current | value, current);
            if (seen == current) return current;
            current = seen;
        }
    }
}
