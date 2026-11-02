using System.Diagnostics;
using System.Text;

namespace RocksDb.Managed.RoundTrip.Harness;

/// <summary>Outcome of a child process run.</summary>
public sealed record ChildResult(int ExitCode, bool TimedOut, bool KilledByParent, IReadOnlyList<string> Stdout, IReadOnlyList<string> Stderr,
    TimeSpan Elapsed, string CommandLine)
{
    public bool Succeeded => ExitCode == 0 && !TimedOut && !KilledByParent;

    /// <summary>"exit code 0", "SIGSEGV (exit 139)", "0xC0000005 STATUS_ACCESS_VIOLATION", ...</summary>
    public string ExitDescription => DescribeExitCode(ExitCode);

    public static string DescribeExitCode(int code)
    {
        if (OperatingSystem.IsWindows())
        {
            uint u = unchecked((uint)code);
            string? name = u switch
            {
                0xC0000005 => "STATUS_ACCESS_VIOLATION",
                0xC0000409 => "STATUS_STACK_BUFFER_OVERRUN / fail fast",
                0xC00000FD => "STATUS_STACK_OVERFLOW",
                0xC0000374 => "STATUS_HEAP_CORRUPTION",
                0xC000001D => "STATUS_ILLEGAL_INSTRUCTION",
                0x80131623 => "COR_E_FAILFAST (Environment.FailFast)",
                0xE0434352 => "unhandled CLR exception",
                0x80004005 => "E_FAIL (unhandled exception)",
                _ => null,
            };
            if (name is not null) return $"0x{u:X8} {name}";
        }
        else if (code > 128 && code < 128 + 65)
        {
            int sig = code - 128;
            string name = sig switch
            {
                4 => "SIGILL", 6 => "SIGABRT", 7 => "SIGBUS", 8 => "SIGFPE", 9 => "SIGKILL", 11 => "SIGSEGV", 15 => "SIGTERM",
                _ => "signal " + sig,
            };
            return $"{name} (exit {code})";
        }
        return code switch
        {
            0 => "exit code 0",
            ChildHost.ExitUsage => "exit code 2 (bad child arguments)",
            ChildHost.ExitVerificationFailed => "exit code 3 (verification failed)",
            ChildHost.ExitError => "exit code 4 (exception)",
            _ => "exit code " + code,
        };
    }

    public static string Tail(IReadOnlyList<string> lines, int max)
    {
        var sb = new StringBuilder();
        int start = Math.Max(0, lines.Count - max);
        if (start > 0) sb.Append("    ... (").Append(start).Append(" earlier lines)\n");
        for (int i = start; i < lines.Count; i++) sb.Append("    ").Append(lines[i]).Append('\n');
        return sb.ToString();
    }

    /// <summary>Full diagnostic for assertion messages.</summary>
    public string Describe()
    {
        var sb = new StringBuilder();
        sb.Append("child ").Append(TimedOut ? "TIMED OUT (killed) after " : "ended after ").Append(Elapsed.TotalSeconds.ToString("F1", System.Globalization.CultureInfo.InvariantCulture))
          .Append("s with ").Append(ExitDescription).Append('\n');
        sb.Append("  command: ").Append(CommandLine).Append('\n');
        sb.Append("  stderr (tail):\n").Append(Tail(Stderr, 40));
        sb.Append("  stdout (tail):\n").Append(Tail(Stdout, 15));
        return sb.ToString();
    }
}

/// <summary>
/// Launches this test executable as a child (<see cref="ChildHost"/>), capturing stdout/stderr line by line,
/// enforcing a timeout (the whole process tree is killed on a hang) and decoding crash exit codes.
/// </summary>
public static class ChildLauncher
{
    /// <summary>How to start this assembly again: (file name, leading arguments).</summary>
    public static (string FileName, string[] Prefix) ResolveHost()
    {
        string dll = typeof(ChildLauncher).Assembly.Location;
        string? processPath = Environment.ProcessPath;
        // Started as "dotnet RocksDb.Managed.RoundTrip.dll" (or dotnet exec): reuse the same host.
        if (processPath is not null && Path.GetFileNameWithoutExtension(processPath).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            return (processPath, ["exec", dll]);
        // Started through the apphost (what "dotnet test" does with Microsoft.Testing.Platform).
        string appHost = Path.ChangeExtension(dll, OperatingSystem.IsWindows() ? ".exe" : null);
        if (File.Exists(appHost)) return (appHost, []);
        string dotnet = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
        return (dotnet, ["exec", dll]);
    }

    /// <summary>The dotnet root of the running runtime (".." of shared/Microsoft.NETCore.App/VERSION).</summary>
    private static string? DotnetRoot()
    {
        string runtimeDir = System.Runtime.InteropServices.RuntimeEnvironment.GetRuntimeDirectory();
        DirectoryInfo? d = new DirectoryInfo(runtimeDir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)).Parent?.Parent?.Parent;
        return d is not null && Directory.Exists(Path.Combine(d.FullName, "shared")) ? d.FullName : null;
    }

    /// <param name="onStdoutLine">Called for each stdout line (on a thread-pool thread); return true to kill the child
    /// (SIGKILL / TerminateProcess of the whole tree), e.g. after enough acknowledged writes.</param>
    public static async Task<ChildResult> RunAsync(IReadOnlyList<string> args, TimeSpan timeout, Func<string, bool>? onStdoutLine)
    {
        (string fileName, string[] prefix) = ResolveHost();
        var psi = new ProcessStartInfo(fileName)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = false,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8,
        };
        // An apphost locates the runtime through DOTNET_ROOT (or global install locations); point it at the runtime
        // this process runs on so the child works with private SDK installs too.
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOTNET_ROOT")) && DotnetRoot() is string root)
            psi.Environment["DOTNET_ROOT"] = root;
        foreach (string a in prefix) psi.ArgumentList.Add(a);
        psi.ArgumentList.Add(ChildHost.ChildArgument);
        foreach (string a in args) psi.ArgumentList.Add(a);
        // Don't let the child inherit test-platform settings that would make it behave like a test host.
        psi.Environment.Remove("TESTINGPLATFORM_UI_LANGUAGE");
        string commandLine = fileName + " " + string.Join(" ", psi.ArgumentList);

        var stdout = new List<string>();
        var stderr = new List<string>();
        using var process = new Process { StartInfo = psi };
        int killRequested = 0;
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is null) return;
            lock (stdout) stdout.Add(e.Data);
            if (onStdoutLine is not null && Volatile.Read(ref killRequested) == 0 && onStdoutLine(e.Data))
            {
                Interlocked.Exchange(ref killRequested, 1);
                try { process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { } // already exited
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is null) return;
            lock (stderr) stderr.Add(e.Data);
        };
        var sw = Stopwatch.StartNew();
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        bool timedOut = false;
        using (var cts = new CancellationTokenSource(timeout))
        {
            try
            {
                await process.WaitForExitAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                timedOut = true;
                try { process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            }
        }
        process.WaitForExit(); // drains the redirected streams
        sw.Stop();
        string[] o, e2;
        lock (stdout) o = [.. stdout];
        lock (stderr) e2 = [.. stderr];
        return new ChildResult(process.ExitCode, timedOut, killRequested != 0, o, e2, sw.Elapsed, commandLine);
    }
}
