using RocksDb.Managed.Env;
using RocksDb.Managed.Wal;

namespace RocksDb.Managed.Db;

/// <summary>
/// <see cref="IManifestRecordReader"/> over the log-format <see cref="LogReader"/> (the MANIFEST uses the WAL's 32KiB
/// block format). Mirrors how VersionSet::Recover reads the descriptor (db/version_set.cc): checksums verified, log
/// number 0, default recovery mode (kTolerateCorruptedTailRecords, so a truncated tail is EOF), and the first
/// corruption reported by the reader (VersionSet::LogReporter) fails recovery with that status.
/// </summary>
public sealed class ManifestLogReader : IManifestRecordReader, IDisposable
{
    private readonly FirstErrorReporter _reporter = new();
    private readonly LogReader _reader;

    /// <param name="file">MANIFEST file; owned by the reader (disposed with it).</param>
    public ManifestLogReader(ISequentialFile file)
    {
        _reader = new LogReader(file, _reporter, checksum: true, logNumber: 0);
    }

    /// <summary>Opens <paramref name="path"/> for sequential reading.</summary>
    public static ManifestLogReader Open(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return new ManifestLogReader(fs.OpenSequential(path));
    }

    /// <inheritdoc/>
    public bool TryReadRecord(out ReadOnlySpan<byte> record)
    {
        bool ok = _reader.TryReadRecord(out record);
        // VersionEditHandlerBase::Iterate stops as soon as the reporter recorded an error, even if a record followed.
        if (_reporter.Error is { } e) throw e;
        return ok;
    }

    public void Dispose() => _reader.Dispose();

    private sealed class FirstErrorReporter : LogReporter
    {
        public RocksDbException? Error { get; private set; }

        public override void Corruption(long bytes, StatusCode code, string reason, ulong logNumber)
        {
            Error ??= code == StatusCode.IOError ? RocksDbException.IOError(reason) : RocksDbException.Corruption(reason);
        }
    }
}

/// <summary>
/// <see cref="IManifestRecordWriter"/> over <see cref="LogWriter"/>: the descriptor log is written like
/// <c>new log::Writer(std::move(file_writer), 0, false)</c> in VersionSet::ProcessManifestWrites (log number 0,
/// non-recyclable records, flushed after each record).
/// </summary>
public sealed class ManifestLogWriter : IManifestRecordWriter, IDisposable, IAsyncDisposable
{
    private readonly LogWriter _log;

    /// <param name="file">New, empty MANIFEST file; owned by the writer (disposed with it).</param>
    public ManifestLogWriter(IWritableFile file)
    {
        _log = new LogWriter(file, 0, recycleLogFiles: false);
    }

    /// <summary>Creates (truncating) the MANIFEST file at <paramref name="path"/>.</summary>
    public static ManifestLogWriter Create(IFileSystem fs, string path)
    {
        ArgumentNullException.ThrowIfNull(fs);
        return new ManifestLogWriter(fs.CreateWritable(path, truncate: true));
    }

    /// <summary>The underlying log writer (e.g. for its file size).</summary>
    public LogWriter Log => _log;

    public ValueTask AddRecordAsync(ReadOnlyMemory<byte> record, CancellationToken cancellationToken) => _log.AddRecordAsync(record, cancellationToken);

    public ValueTask SyncAsync(CancellationToken cancellationToken) => _log.SyncAsync(cancellationToken);

    public void Dispose() => _log.Dispose();

    public ValueTask DisposeAsync() => _log.DisposeAsync();
}
