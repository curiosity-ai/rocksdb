using System.Runtime.InteropServices;

namespace RocksDb.Managed.Env;

/// <summary>
/// CPU time consumed by the calling OS thread: SystemClock::CPUMicros (env/env_posix.cc / port/win/env_win.cc:
/// clock_gettime(CLOCK_THREAD_CPUTIME_ID), GetThreadTimes on Windows). .NET has no managed per-thread CPU clock, so
/// this is a P/Invoke per platform; 0 where there is none (upstream's default CPUMicros also returns 0).
/// <para>
/// Only meaningful between two calls made on the same OS thread, i.e. around synchronous code (no await in between).
/// </para>
/// </summary>
internal static partial class ThreadCpuClock
{
    // CLOCK_THREAD_CPUTIME_ID: 3 on Linux (every architecture), 16 on macOS (_CLOCK_THREAD_CPUTIME_ID).
    private const int LinuxClockThreadCpuTimeId = 3;
    private const int MacClockThreadCpuTimeId = 16;

    private static readonly int s_kind = Probe();

    /// <summary>struct timespec of the 64-bit and 32-bit ABIs (time_t and long are both pointer-sized there).</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Timespec
    {
        public nint tv_sec;
        public nint tv_nsec;
    }

    [LibraryImport("libc", EntryPoint = "clock_gettime")]
    private static partial int clock_gettime(int clockId, out Timespec ts);

    [LibraryImport("kernel32")]
    private static partial nint GetCurrentThread();

    [LibraryImport("kernel32")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetThreadTimes(nint thread, out long creationTime, out long exitTime, out long kernelTime, out long userTime);

    /// <summary>True when the platform has a per-thread CPU clock (<see cref="CpuMicros"/> can be non-zero).</summary>
    public static bool IsSupported => s_kind != 0;

    /// <summary>CPU time (user + system) of the calling thread in microseconds; 0 when unsupported.</summary>
    public static ulong CpuMicros()
    {
        switch (s_kind)
        {
            case 1:
            case 2:
                if (clock_gettime(s_kind == 1 ? LinuxClockThreadCpuTimeId : MacClockThreadCpuTimeId, out Timespec ts) != 0) return 0;
                return (ulong)(long)ts.tv_sec * 1_000_000UL + (ulong)(long)ts.tv_nsec / 1000UL;
            case 3:
                // FILETIME units of 100 ns.
                return GetThreadTimes(GetCurrentThread(), out _, out _, out long kernel, out long user) ? (ulong)(kernel + user) / 10UL : 0;
            default:
                return 0;
        }
    }

    private static int Probe()
    {
        int kind = OperatingSystem.IsLinux() ? 1 : OperatingSystem.IsMacOS() ? 2 : OperatingSystem.IsWindows() ? 3 : 0;
        if (kind == 0) return 0;
        try
        {
            if (kind == 3) return GetThreadTimes(GetCurrentThread(), out _, out _, out _, out _) ? 3 : 0;
            return clock_gettime(kind == 1 ? LinuxClockThreadCpuTimeId : MacClockThreadCpuTimeId, out _) == 0 ? kind : 0;
        }
        catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
        {
            return 0;
        }
    }
}
