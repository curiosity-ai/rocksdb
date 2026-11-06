namespace RocksDb.Managed;

/// <summary>
/// Destination of the DB's informational log ("info LOG"): open summary and options dump, flush and compaction events,
/// errors and periodic stats dumps. Port of rocksdb::Logger (include/rocksdb/env.h, env/env.cc Logger::Logv).
/// <para>
/// Assign an implementation to <see cref="DbOptions.InfoLog"/> to receive the DB's messages (e.g. to forward them to
/// <c>Microsoft.Extensions.Logging</c>); when it is null the DB writes a <c>LOG</c> file in the DB directory (or
/// <see cref="DbOptions.DbLogDir"/>) like native RocksDB. Implementations must be thread-safe; <see cref="LogCore"/>
/// may be called from foreground and background threads and should not block for long. The DB does not dispose a
/// user-supplied logger.
/// </para>
/// </summary>
public abstract class InfoLogger : IDisposable
{
    private static readonly string[] s_levelNames = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"];

    protected InfoLogger(InfoLogLevel level) => Level = level;

    /// <summary>Messages below this level are dropped (Logger::GetInfoLogLevel). Header messages are never dropped.</summary>
    public InfoLogLevel Level { get; set; }

    /// <summary>Whether a message of <paramref name="level"/> would be logged.</summary>
    public bool IsEnabled(InfoLogLevel level) => level >= Level;

    /// <summary>
    /// Logs <paramref name="message"/> at <paramref name="level"/> (Logger::Logv(level, ...)): INFO messages are written
    /// as is, HEADER messages through <see cref="LogHeader"/>, other levels get a "[LEVEL] " prefix; WARN and above are
    /// flushed immediately.
    /// </summary>
    public void Log(InfoLogLevel level, string message)
    {
        if (level < Level) return;
        switch (level)
        {
            case InfoLogLevel.Info:
                LogCore(level, message);
                break;
            case InfoLogLevel.Header:
                LogHeader(message);
                break;
            default:
                LogCore(level, "[" + s_levelNames[(int)level] + "] " + message);
                break;
        }
        if (level is >= InfoLogLevel.Warn and not InfoLogLevel.Header) Flush();
    }

    /// <summary>
    /// Logs the contents of <paramref name="message"/> at INFO level. The DB formats its flush / compaction / file events
    /// into a reused builder; the DB's own LOG file writes them without an intermediate message string, other loggers
    /// receive <see cref="StringBuilder.ToString()"/> through <see cref="LogCore"/>.
    /// </summary>
    internal virtual void LogInfo(System.Text.StringBuilder message)
    {
        if (InfoLogLevel.Info < Level) return;
        LogCore(InfoLogLevel.Info, message.ToString());
    }

    /// <summary>Writes a header line (Logger::LogHeader; by default the same as an INFO line). Not filtered by level.</summary>
    public virtual void LogHeader(string message) => LogCore(InfoLogLevel.Header, message);

    /// <summary>Writes one already formatted message (level prefix included).</summary>
    protected abstract void LogCore(InfoLogLevel level, string message);

    /// <summary>Pushes buffered messages to their destination.</summary>
    public virtual void Flush() { }

    /// <summary>Async <see cref="Flush"/>.</summary>
    public virtual ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return default;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing) { }
}
