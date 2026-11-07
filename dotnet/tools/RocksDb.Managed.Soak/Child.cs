using System.Diagnostics;
using System.Runtime.InteropServices;

namespace RocksDb.Managed.Soak;

/// <summary>A child process of this executable (worker, verifier, native digest...), with its output in a log file.</summary>
internal sealed class ChildProcess : IDisposable
{
    private readonly Process _p;
    private readonly StreamWriter? _log;
    private readonly Lock _logLock = new();
    private readonly TaskCompletionSource<double> _opened = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Queue<string> _tail = new();
    private readonly TaskCompletionSource _outputDone = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _streams = 2;

    private ChildProcess(Process p, StreamWriter? log)
    {
        _p = p;
        _log = log;
    }

    public int Pid => _p.Id;

    /// <summary>Completes with the open time (ms) when the worker prints "OPENED ms".</summary>
    public Task<double> Opened => _opened.Task;

    public bool HasExited => _p.HasExited;

    public string Tail
    {
        get
        {
            lock (_logLock) return string.Join("\n", _tail);
        }
    }

    public static (string Exe, List<string> Prefix) Self()
    {
        string exe = Environment.ProcessPath ?? "dotnet";
        var prefix = new List<string>();
        if (Path.GetFileNameWithoutExtension(exe) == "dotnet") prefix.Add(typeof(ChildProcess).Assembly.Location);
        return (exe, prefix);
    }

    public static ChildProcess Start(IEnumerable<string> args, string? logFile)
    {
        (string exe, List<string> prefix) = Self();
        var psi = new ProcessStartInfo(exe)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (string a in prefix) psi.ArgumentList.Add(a);
        foreach (string a in args) psi.ArgumentList.Add(a);
        StreamWriter? log = logFile is null ? null : new StreamWriter(new FileStream(logFile, FileMode.Append, FileAccess.Write, FileShare.ReadWrite)) { AutoFlush = true };
        var p = new Process { StartInfo = psi };
        var c = new ChildProcess(p, log);
        p.OutputDataReceived += (_, e) => c.OnLine(e.Data, false);
        p.ErrorDataReceived += (_, e) => c.OnLine(e.Data, true);
        p.Start();
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        return c;
    }

    private void OnLine(string? line, bool err)
    {
        if (line is null)
        {
            if (Interlocked.Decrement(ref _streams) == 0) _outputDone.TrySetResult();
            return;
        }
        lock (_logLock)
        {
            _log?.WriteLine(err ? "[stderr] " + line : line);
            _tail.Enqueue(line);
            while (_tail.Count > 40) _tail.Dequeue();
        }
        if (line.StartsWith("OPENED ", StringComparison.Ordinal) &&
            double.TryParse(line.AsSpan(7), System.Globalization.CultureInfo.InvariantCulture, out double ms))
            _opened.TrySetResult(ms);
    }

    public void Send(string line)
    {
        try
        {
            _p.StandardInput.WriteLine(line);
            _p.StandardInput.Flush();
        }
        catch (IOException)
        {
            // the child is gone
        }
    }

    /// <summary>SIGKILL.</summary>
    public void Kill()
    {
        try { _p.Kill(); }
        catch (InvalidOperationException) { }
    }

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int SysKill(int pid, int sig);

    public const int SigStop = 19;
    public const int SigCont = 18;

    public void Signal(int sig)
    {
        if (!_p.HasExited) _ = SysKill(_p.Id, sig);
    }

    /// <summary>Waits for the exit; returns the exit code, or null on timeout.</summary>
    public async Task<int?> WaitAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await _p.WaitForExitAsync(cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        await Task.WhenAny(_outputDone.Task, Task.Delay(2000)).ConfigureAwait(false);
        return _p.ExitCode;
    }

    public void Dispose()
    {
        if (!_p.HasExited) Kill();
        _p.Dispose();
        lock (_logLock) _log?.Dispose();
    }
}

internal static class Child
{
    /// <summary>Runs a child to completion; a timeout kills it (returns -1).</summary>
    public static (int Code, string Tail) Run(IEnumerable<string> args, TimeSpan timeout, string? logFile)
    {
        using ChildProcess c = ChildProcess.Start(args, logFile);
        int? code = c.WaitAsync(timeout).GetAwaiter().GetResult();
        if (code is null)
        {
            c.Kill();
            c.WaitAsync(TimeSpan.FromSeconds(30)).GetAwaiter().GetResult();
            return (-1, "timed out after " + timeout + "\n" + c.Tail);
        }
        return (code.Value, c.Tail);
    }

    public static string Describe(int code) => code switch
    {
        -1 => "a timeout (hang)",
        0 => "exit 0",
        137 => "SIGKILL (137)",
        134 => "SIGABRT (134)",
        139 => "SIGSEGV (139)",
        _ => "exit " + code,
    };
}
