using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Logging;

/// <summary>
/// The DB's own info LOG file. Port of CreateLoggerFromOptions (logging/auto_roll_logger.cc), AutoRollLogger and the
/// line format of EnvLogger / PosixLogger::Logv (logging/env_logger.h):
/// <c>yyyy/MM/dd-HH:mm:ss.uuuuuu &lt;thread id hex&gt; message</c>.
/// <list type="bullet">
/// <item>An existing <c>LOG</c> is renamed to <c>LOG.old.&lt;micros&gt;</c> when the logger is created.</item>
/// <item>With <c>max_log_file_size</c> or <c>log_file_time_to_roll</c> set, the file is rolled the same way when it
/// grows past the size or gets older than the period; header lines are replayed into the new file.</item>
/// <item>At most <c>keep_log_file_num</c> old files are kept; the oldest are deleted.</item>
/// </list>
/// Messages are queued and written by a background task, so logging never does file I/O on the caller's thread;
/// <see cref="Flush"/> (and WARN-or-higher messages) drain the queue synchronously.
/// </summary>
internal sealed class FileInfoLogger : InfoLogger
{
    private readonly IFileSystem _fs;
    private readonly string _dbname;
    private readonly string _dbAbsolutePath;
    private readonly string _logDir;
    private readonly string _fileName;
    private readonly ulong _maxLogFileSize;
    private readonly ulong _logFileTimeToRoll;
    private readonly ulong _keepLogFileNum;

    private readonly ConcurrentQueue<string> _queue = new();
    private readonly SemaphoreSlim _signal = new(0, int.MaxValue);
    private readonly Lock _writeLock = new();        // serializes writes, rolls and close
    private readonly List<string> _headers = new();   // guarded by _writeLock
    private readonly Task _writer;
    private IWritableFile? _file;                     // guarded by _writeLock
    private long _fileCreatedTicks;
    private int _pendingSignals;
    private volatile bool _closed;

    private FileInfoLogger(IFileSystem fs, string dbname, string dbAbsolutePath, string logDir, DbOptions options)
        : base(options.InfoLogLevel)
    {
        _fs = fs;
        _dbname = dbname;
        _dbAbsolutePath = dbAbsolutePath;
        _logDir = logDir;
        _fileName = FileNames.InfoLogFileName(dbname, dbAbsolutePath, logDir);
        _maxLogFileSize = options.MaxLogFileSize;
        _logFileTimeToRoll = options.LogFileTimeToRoll;
        _keepLogFileNum = Math.Max(1, options.KeepLogFileNum);
        _writer = Task.Factory.StartNew(static s => ((FileInfoLogger)s!).WriterLoopAsync(), this, CancellationToken.None,
            TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
    }

    /// <summary>
    /// Port of CreateLoggerFromOptions: creates the log directory, renames an existing LOG to LOG.old.&lt;micros&gt;,
    /// trims old files and opens a new LOG.
    /// </summary>
    public static FileInfoLogger Create(IFileSystem fs, string dbname, DbOptions options)
    {
        string logDir = options.DbLogDir.TrimEnd('/');
        string absolute = dbname;
        try { absolute = System.IO.Path.GetFullPath(dbname); } catch (Exception e) when (e is ArgumentException or NotSupportedException or PathTooLongException) { }
        fs.CreateDirectoryIfMissing(logDir.Length == 0 ? dbname : logDir);
        var logger = new FileInfoLogger(fs, dbname, absolute, logDir, options);
        lock (logger._writeLock) logger.OpenNewFileLocked();
        return logger;
    }

    /// <summary>The path of the current LOG file.</summary>
    public string FileName => _fileName;

    private static ulong NowMicros() => (ulong)(DateTime.UtcNow - DateTime.UnixEpoch).Ticks / 10;

    /// <summary>Renames the current LOG (if any) to LOG.old.&lt;micros&gt;, trims old files and creates a new LOG. REQUIRES: _writeLock.</summary>
    private void OpenNewFileLocked()
    {
        _file?.Dispose();
        _file = null;
        if (_fs.FileExists(_fileName))
        {
            try
            {
                _fs.RenameFile(_fileName, FileNames.OldInfoLogFileName(_dbname, NowMicros(), _dbAbsolutePath, _logDir));
            }
            catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
            {
                // Keep appending to a fresh file anyway (upstream ignores the rename status).
            }
        }
        TrimOldLogFiles();
        _file = _fs.CreateWritable(_fileName, truncate: true);
        _fileCreatedTicks = Environment.TickCount64;
    }

    /// <summary>AutoRollLogger::TrimOldLogFiles / DBImpl info-log purge: keeps the newest keep_log_file_num old LOG files.</summary>
    private void TrimOldLogFiles()
    {
        string dir = _logDir.Length == 0 ? _dbname : _logDir;
        string prefix = FileNames.InfoLogPrefix(_logDir.Length != 0, _dbAbsolutePath) + ".old.";
        var old = new List<(ulong Ts, string Name)>();
        try
        {
            foreach (string child in _fs.GetChildren(dir))
            {
                if (!child.StartsWith(prefix, StringComparison.Ordinal)) continue;
                if (ulong.TryParse(child.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out ulong ts)) old.Add((ts, child));
            }
        }
        catch (Exception e) when (e is IOException or RocksDbException)
        {
            return;
        }
        if ((ulong)old.Count <= _keepLogFileNum) return;
        old.Sort(static (a, b) => a.Ts.CompareTo(b.Ts));
        for (int i = 0; i < old.Count - (int)_keepLogFileNum; i++)
        {
            try { _fs.DeleteFile(dir + "/" + old[i].Name); }
            catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException) { }
        }
    }

    // ---- logging ------------------------------------------------------------------------------------

    protected override void LogCore(InfoLogLevel level, string message)
    {
        if (_closed) return;
        string line = FormatLine(message);
        if (level == InfoLogLevel.Header)
        {
            lock (_writeLock) _headers.Add(line);
        }
        Enqueue(line);
    }

    internal override void LogInfo(StringBuilder message)
    {
        if (InfoLogLevel.Info < Level || _closed) return;
        Enqueue(FormatLine(message));
    }

    private void Enqueue(string line)
    {
        _queue.Enqueue(line);
        // Wake the writer at most once per batch of messages.
        if (Interlocked.Exchange(ref _pendingSignals, 1) == 0) _signal.Release();
    }

    /// <summary>Length of the "yyyy/MM/dd-HH:mm:ss.ffffff" timestamp.</summary>
    private const int TimestampLength = 26;

    /// <summary>EnvLogger::Logv header: local time with microseconds and the thread id in hex.</summary>
    private static string FormatLine(string message)
    {
        bool newline = message.Length == 0 || message[^1] != '\n';
        int tid = Environment.CurrentManagedThreadId;
        int length = TimestampLength + 1 + HexLength(tid) + 1 + message.Length + (newline ? 1 : 0);
        return string.Create(length, (DateTime.Now, tid, message, newline), static (span, st) =>
        {
            int pos = WritePrefix(span, st.Item1, st.tid);
            st.message.CopyTo(span[pos..]);
            if (st.newline) span[^1] = '\n';
        });
    }

    /// <summary><see cref="FormatLine(string)"/> for a message held in a builder (no intermediate message string).</summary>
    private static string FormatLine(StringBuilder message)
    {
        int n = message.Length;
        bool newline = n == 0 || message[n - 1] != '\n';
        int tid = Environment.CurrentManagedThreadId;
        int length = TimestampLength + 1 + HexLength(tid) + 1 + n + (newline ? 1 : 0);
        return string.Create(length, (DateTime.Now, tid, message, newline), static (span, st) =>
        {
            int pos = WritePrefix(span, st.Item1, st.tid);
            st.message.CopyTo(0, span[pos..], st.message.Length);
            if (st.newline) span[^1] = '\n';
        });
    }

    private static int HexLength(int value) => value <= 0 ? 1 : (32 - System.Numerics.BitOperations.LeadingZeroCount((uint)value) + 3) / 4;

    /// <summary>Writes "&lt;timestamp&gt; &lt;thread id hex&gt; " and returns its length.</summary>
    private static int WritePrefix(Span<char> span, DateTime now, int tid)
    {
        now.TryFormat(span, out int written, "yyyy/MM/dd-HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        span[written++] = ' ';
        tid.TryFormat(span[written..], out int w, "x", CultureInfo.InvariantCulture);
        written += w;
        span[written++] = ' ';
        return written;
    }

    private async Task WriterLoopAsync()
    {
        while (true)
        {
            await _signal.WaitAsync().ConfigureAwait(false);
            Volatile.Write(ref _pendingSignals, 0);
            Drain(flush: false);
            if (_closed && _queue.IsEmpty) return;
        }
    }

    /// <summary>Writes every queued line (rolling as needed); with <paramref name="flush"/> also flushes the file.</summary>
    private void Drain(bool flush)
    {
        lock (_writeLock)
        {
            IWritableFile? f = _file;
            if (f is null) return;
            try
            {
                bool wrote = false;
                while (_queue.TryDequeue(out string? line))
                {
                    WriteLine(_file!, line);
                    wrote = true;
                    MaybeRollLocked();
                }
                if (wrote || flush) _file?.Flush();
            }
            catch (Exception e) when (e is IOException or RocksDbException or ObjectDisposedException)
            {
                // Logging must never fail the DB; drop what cannot be written.
                while (_queue.TryDequeue(out _)) { }
            }
        }
    }

    private static void WriteLine(IWritableFile f, string line)
    {
        int max = Encoding.UTF8.GetMaxByteCount(line.Length);
        byte[]? rented = null;
        Span<byte> buf = max <= 1024 ? stackalloc byte[1024] : (rented = System.Buffers.ArrayPool<byte>.Shared.Rent(max));
        try
        {
            int n = Encoding.UTF8.GetBytes(line, buf);
            f.Append(buf[..n]);
        }
        finally
        {
            if (rented is not null) System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>AutoRollLogger::Logv roll check (size and time). REQUIRES: _writeLock.</summary>
    private void MaybeRollLocked()
    {
        bool bySize = _maxLogFileSize > 0 && (ulong)_file!.Size >= _maxLogFileSize;
        bool byTime = _logFileTimeToRoll > 0 && (ulong)(Environment.TickCount64 - _fileCreatedTicks) / 1000 >= _logFileTimeToRoll;
        if (!bySize && !byTime) return;
        OpenNewFileLocked();
        // Replay the headers (options dump etc.) at the top of the new file, like AutoRollLogger::RollLogFile.
        foreach (string h in _headers) WriteLine(_file!, h);
    }

    public override void Flush() => Drain(flush: true);

    protected override void Dispose(bool disposing)
    {
        if (_closed) return;
        _closed = true;
        _signal.Release();
        try { _writer.Wait(TimeSpan.FromSeconds(5)); } catch (AggregateException) { }
        Drain(flush: true);
        lock (_writeLock)
        {
            try { _file?.Dispose(); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
            _file = null;
        }
    }
}
