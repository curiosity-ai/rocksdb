using System.Diagnostics;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>
/// In the test host, a failed <see cref="Debug.Assert(bool)"/> in the managed engine would end in
/// Environment.FailFast and take down the whole test run. Once a managed script runs in-process this listener
/// replaces the trace listeners (which receive Debug failures once System.Diagnostics.Trace is initialized) and turns
/// assertion failures into <see cref="DebugAssertException"/>s, which fail just that test. In a child process the
/// default FailFast behaviour is kept and reported as SIGABRT.
/// </summary>
public sealed class ThrowingTraceListener : TraceListener
{
    private static int s_installed;

    public static void Install()
    {
        if (Interlocked.Exchange(ref s_installed, 1) != 0) return;
        lock (Trace.Listeners)
        {
            Trace.Listeners.Clear();
            Trace.Listeners.Add(new ThrowingTraceListener());
        }
    }

    public override void Fail(string? message, string? detailMessage) =>
        throw new DebugAssertException("Debug.Assert failed in the managed engine: " + message + (detailMessage is null ? "" : " " + detailMessage));

    public override void Write(string? message) { }

    public override void WriteLine(string? message) { }
}

public sealed class DebugAssertException(string message) : Exception(message);
