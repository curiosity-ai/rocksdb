using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using RocksDb.Managed.Env;

namespace RocksDb.Managed.Db;

/// <summary>
/// The file I/O notifications of the DB's listeners: ports of the NotifyOnFile*Finish / NotifyOnIOError helpers of
/// RandomAccessFileReader (file/random_access_file_reader.{h,cc}, random_access_file_reader_sync_and_async.h),
/// WritableFileWriter (file/writable_file_writer.{h,cc}) and SequentialFileReader (file/sequence_file_reader.{h,cc}).
/// <para>
/// Upstream gives the DB's listener list to the readers and writers of the files it reports on, and each keeps the
/// listeners whose ShouldBeNotifiedOnFileIO() is true; with none left, ShouldNotifyListeners() is false and no timestamp
/// is taken. Here the DB keeps one notifier, created at open when at least one listener wants file I/O
/// (<see cref="Create"/> returns null otherwise), and the files of the sites that report (table cache, blob reader, table
/// and blob writers, WAL writer, MANIFEST reader and writer, GetUpdatesSince) are wrapped by <see cref="Wrap(FileIoNotifier?,
/// IRandomAccessFile, string, Temperature)"/> and its overloads only when it exists: without such a listener no file is
/// wrapped and the I/O paths are unchanged.
/// </para>
/// <para>
/// Writes: upstream reports each write of the WritableFileWriter's buffer to the file system. <see cref="LocalWritableFile"/>
/// is that writer, so it reports its own physical writes and range syncs (<see cref="WritableFileOptions.FileIoNotifier"/>);
/// for files of other file systems, which buffer internally (or not at all), the wrapper reports every append.
/// </para>
/// </summary>
internal sealed class FileIoNotifier
{
    private readonly EventListener[] _listeners;
    private readonly Action<string> _logWarn;

    private FileIoNotifier(EventListener[] listeners, Action<string> logWarn)
    {
        _listeners = listeners;
        _logWarn = logWarn;
    }

    /// <summary>The listeners notified on file I/O (tests).</summary>
    internal int ListenerCount => _listeners.Length;

    /// <summary>
    /// The notifier of the listeners of <paramref name="listeners"/> whose ShouldBeNotifiedOnFileIO() returns true, or null
    /// when there is none (the WritableFileWriter / RandomAccessFileReader constructors' filter). A listener whose
    /// ShouldBeNotifiedOnFileIO throws is not notified.
    /// </summary>
    internal static FileIoNotifier? Create(IReadOnlyList<EventListener> listeners, Action<string> logWarn)
    {
        List<EventListener>? wanted = null;
        foreach (EventListener l in listeners)
        {
            bool wants;
            try
            {
                wants = l.ShouldBeNotifiedOnFileIO();
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                logWarn("EventListener.ShouldBeNotifiedOnFileIO threw: " + e.Message);
                wants = false;
            }
            if (wants) (wanted ??= new List<EventListener>()).Add(l);
        }
        return wanted is null ? null : new FileIoNotifier(wanted.ToArray(), logWarn);
    }

    /// <summary>FileOperationInfo::StartNow: the system time and the monotonic timestamp of the start of an operation.</summary>
    internal static long Start(out DateTime startTime)
    {
        startTime = DateTime.UtcNow;
        return Stopwatch.GetTimestamp();
    }

    /// <summary>
    /// The NotifyOnFile*Finish helpers: builds the FileOperationInfo (no allocation for a successful operation) and calls the
    /// callback of <paramref name="type"/> of every listener. Listener exceptions are logged and swallowed.
    /// </summary>
    internal void Finish(FileOperationType type, string path, Temperature temperature, ulong offset, ulong length, DateTime startTime,
        long startTimestamp, Exception? failure)
    {
        TimeSpan duration = Stopwatch.GetElapsedTime(startTimestamp);
        var info = new FileOperationInfo(type, path, temperature, offset, length, startTime, duration,
            failure is null ? null : RocksDbException.FromException(failure));
        foreach (EventListener l in _listeners)
        {
            try
            {
                switch (type)
                {
                    case FileOperationType.Read:
                        l.OnFileReadFinish(in info);
                        break;
                    case FileOperationType.Write:
                        l.OnFileWriteFinish(in info);
                        break;
                    case FileOperationType.Flush:
                        l.OnFileFlushFinish(in info);
                        break;
                    case FileOperationType.Sync:
                    case FileOperationType.Fsync:
                        l.OnFileSyncFinish(in info);
                        break;
                    case FileOperationType.RangeSync:
                        l.OnFileRangeSyncFinish(in info);
                        break;
                    case FileOperationType.Truncate:
                        l.OnFileTruncateFinish(in info);
                        break;
                    case FileOperationType.Close:
                        l.OnFileCloseFinish(in info);
                        break;
                }
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                LogListenerFailure(type, e);
            }
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void LogListenerFailure(FileOperationType type, Exception e) =>
        _logWarn("EventListener." + CallbackName(type) + " threw: " + e.Message);

    private static string CallbackName(FileOperationType type) => type switch
    {
        FileOperationType.Read => nameof(EventListener.OnFileReadFinish),
        FileOperationType.Write => nameof(EventListener.OnFileWriteFinish),
        FileOperationType.Flush => nameof(EventListener.OnFileFlushFinish),
        FileOperationType.Sync or FileOperationType.Fsync => nameof(EventListener.OnFileSyncFinish),
        FileOperationType.RangeSync => nameof(EventListener.OnFileRangeSyncFinish),
        FileOperationType.Truncate => nameof(EventListener.OnFileTruncateFinish),
        _ => nameof(EventListener.OnFileCloseFinish),
    };

    /// <summary>NotifyOnIOError: <see cref="EventListener.OnIOError"/> of every listener.</summary>
    internal void IoError(Exception failure, FileOperationType operation, string path, ulong length, ulong offset)
    {
        var info = new IOErrorInfo(RocksDbException.FromException(failure), operation, path, length, offset);
        foreach (EventListener l in _listeners)
        {
            try
            {
                l.OnIOError(info);
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                _logWarn("EventListener.OnIOError threw: " + e.Message);
            }
        }
    }

    /// <summary>
    /// <paramref name="file"/> reporting its reads to <paramref name="notifier"/> (RandomAccessFileReader constructed with
    /// the listeners), or <paramref name="file"/> itself when there is no notifier.
    /// </summary>
    internal static IRandomAccessFile Wrap(FileIoNotifier? notifier, IRandomAccessFile file, string path, Temperature temperature) =>
        notifier is null ? file : new NotifyingRandomAccessFile(file, notifier, path, temperature);

    /// <summary><paramref name="file"/> reporting its reads (SequentialFileReader with the listeners), or itself.</summary>
    internal static ISequentialFile Wrap(FileIoNotifier? notifier, ISequentialFile file, string path) =>
        notifier is null ? file : new NotifyingSequentialFile(file, notifier, path);

    /// <summary>
    /// <paramref name="file"/> reporting its writes, flushes, syncs and close (WritableFileWriter with the listeners), or
    /// itself. When <paramref name="file"/> reports its own physical writes (<see cref="IFileIoWriteReporter"/>, a
    /// <see cref="LocalWritableFile"/> created with <see cref="WritableFileOptions.FileIoNotifier"/>) its appends are not
    /// reported again.
    /// </summary>
    internal static IWritableFile Wrap(FileIoNotifier? notifier, IWritableFile file, string path) =>
        notifier is null ? file : new NotifyingWritableFile(file, notifier, path);

    /// <summary>
    /// The file system of the MANIFEST accesses (VersionSet, recovery): MANIFEST files are opened through the wrappers,
    /// every other file and operation goes to <paramref name="inner"/> unchanged (upstream gives the listeners to the
    /// MANIFEST readers and writer only, not to CURRENT or the temporary files).
    /// </summary>
    internal static IFileSystem ManifestFileSystem(FileIoNotifier? notifier, IFileSystem inner) =>
        notifier is null ? inner : new ManifestNotifyingFileSystem(inner, notifier);

    private static bool IsManifest(string path) =>
        FileNames.TryParseFileName(Path.GetFileName(path), out _, out FileType type) && type == FileType.DescriptorFile;

    private sealed class ManifestNotifyingFileSystem(IFileSystem inner, FileIoNotifier notifier) : IFileSystem
    {
        public IWritableFile CreateWritable(string path, bool truncate = true) =>
            IsManifest(path) ? Wrap(notifier, inner.CreateWritable(path, truncate, new WritableFileOptions { FileIoNotifier = notifier }), path)
                : inner.CreateWritable(path, truncate);

        public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) =>
            IsManifest(path) ? Wrap(notifier, inner.CreateWritable(path, truncate, options.WithFileIoNotifier(notifier)), path)
                : inner.CreateWritable(path, truncate, options);

        public IWritableFile ReopenWritable(string path, WritableFileOptions options) =>
            IsManifest(path) ? Wrap(notifier, inner.ReopenWritable(path, options.WithFileIoNotifier(notifier)), path) : inner.ReopenWritable(path, options);

        public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) => inner.ReuseWritable(path, oldPath, options);
        public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
        public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => inner.OpenRandomAccess(path, options);

        public ISequentialFile OpenSequential(string path) =>
            IsManifest(path) ? Wrap(notifier, inner.OpenSequential(path), path) : inner.OpenSequential(path);

        public ISequentialFile OpenSequential(string path, SequentialFileOptions options) =>
            IsManifest(path) ? Wrap(notifier, inner.OpenSequential(path, options), path) : inner.OpenSequential(path, options);

        public bool FileExists(string path) => inner.FileExists(path);
        public long GetFileSize(string path) => inner.GetFileSize(path);
        public void DeleteFile(string path) => inner.DeleteFile(path);
        public void RenameFile(string source, string target) => inner.RenameFile(source, target);
        public void CreateDirectoryIfMissing(string path) => inner.CreateDirectoryIfMissing(path);
        public IEnumerable<string> GetChildren(string directory) => inner.GetChildren(directory);
        public IDisposable LockFile(string path) => inner.LockFile(path);
        public void SyncDirectory(string path) => inner.SyncDirectory(path);
        public bool DirectoryExists(string path) => inner.DirectoryExists(path);
        public void DeleteDirectory(string path) => inner.DeleteDirectory(path);
        public void LinkFile(string source, string target) => inner.LinkFile(source, target);
        public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);
        public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
        public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
    }
}

/// <summary>
/// A writable file that reports its physical writes and range syncs to the <see cref="FileIoNotifier"/> of its
/// <see cref="WritableFileOptions"/> itself (implemented by <see cref="LocalWritableFile"/> and forwarded by wrappers).
/// </summary>
internal interface IFileIoWriteReporter
{
    bool ReportsFileIoWrites { get; }
}

/// <summary>
/// RandomAccessFileReader's notifications (random_access_file_reader_sync_and_async.h Read, ReadAsync's callback): every
/// read is reported with its offset and the bytes read; a failed read also raises OnIOError(kRead).
/// </summary>
internal sealed class NotifyingRandomAccessFile(IRandomAccessFile inner, FileIoNotifier notifier, string path, Temperature temperature) : IRandomAccessFile
{
    public long Length => inner.Length;

    public int Read(long offset, Span<byte> destination)
    {
        long start = FileIoNotifier.Start(out DateTime startTime);
        int n;
        try
        {
            n = inner.Read(offset, destination);
        }
        catch (Exception e)
        {
            Failed(e, offset, startTime, start);
            throw;
        }
        notifier.Finish(FileOperationType.Read, path, temperature, (ulong)offset, (ulong)n, startTime, start, null);
        return n;
    }

    // "NotifyOnFileReadFinish(offset + pos, tmp_result.size(), ...); if (!io_s.ok()) NotifyOnIOError(io_s, kRead, file_name(),
    // tmp_result.size(), offset + pos)": nothing was read by a read that threw.
    private void Failed(Exception e, long offset, DateTime startTime, long start)
    {
        notifier.Finish(FileOperationType.Read, path, temperature, (ulong)offset, 0, startTime, start, e);
        notifier.IoError(e, FileOperationType.Read, path, 0, (ulong)offset);
    }

    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        long start = FileIoNotifier.Start(out DateTime startTime);
        ValueTask<int> t;
        try
        {
            t = inner.ReadAsync(offset, destination, cancellationToken);
        }
        catch (Exception e)
        {
            Failed(e, offset, startTime, start);
            throw;
        }
        if (t.IsCompletedSuccessfully)
        {
            int n = t.Result;
            notifier.Finish(FileOperationType.Read, path, temperature, (ulong)offset, (ulong)n, startTime, start, null);
            return new ValueTask<int>(n);
        }
        return Awaited(t, offset, startTime, start);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> Awaited(ValueTask<int> t, long offset, DateTime startTime, long start)
    {
        int n;
        try
        {
            n = await t.ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Failed(e, offset, startTime, start);
            throw;
        }
        notifier.Finish(FileOperationType.Read, path, temperature, (ulong)offset, (ulong)n, startTime, start, null);
        return n;
    }

    /// <summary>RandomAccessFileReader::Prefetch is not reported.</summary>
    public bool Prefetch(long offset, long length) => inner.Prefetch(offset, length);

    public void Dispose() => inner.Dispose();
}

/// <summary>
/// SequentialFileReader's notifications (sequence_file_reader.cc Read): every read is reported with the file offset it
/// started at (offset_, advanced by reads and Skip) and the bytes read. SequentialFileReader does not raise OnIOError.
/// </summary>
internal sealed class NotifyingSequentialFile(ISequentialFile inner, FileIoNotifier notifier, string path) : ISequentialFile
{
    private long _offset;

    public int Read(Span<byte> destination)
    {
        long start = FileIoNotifier.Start(out DateTime startTime);
        int n;
        try
        {
            n = inner.Read(destination);
        }
        catch (Exception e)
        {
            notifier.Finish(FileOperationType.Read, path, Temperature.Unknown, (ulong)_offset, 0, startTime, start, e);
            throw;
        }
        notifier.Finish(FileOperationType.Read, path, Temperature.Unknown, (ulong)_offset, (ulong)n, startTime, start, null);
        _offset += n;
        return n;
    }

    public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        long start = FileIoNotifier.Start(out DateTime startTime);
        int n;
        try
        {
            n = await inner.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            notifier.Finish(FileOperationType.Read, path, Temperature.Unknown, (ulong)_offset, 0, startTime, start, e);
            throw;
        }
        notifier.Finish(FileOperationType.Read, path, Temperature.Unknown, (ulong)_offset, (ulong)n, startTime, start, null);
        _offset += n;
        return n;
    }

    /// <summary>SequentialFileReader::Skip: not reported; the offset of the next read moves.</summary>
    public void Skip(long bytes)
    {
        inner.Skip(bytes);
        _offset += bytes;
    }

    public void Dispose() => inner.Dispose();
}

/// <summary>
/// WritableFileWriter's notifications (writable_file_writer.cc): writes (when the wrapped file does not report its own),
/// Flush (OnFileFlushFinish), Sync (a flush, then OnFileSyncFinish), and Close (a flush, then OnFileCloseFinish with the
/// status of that flush, and OnIOError(kClose) when the close itself failed). After a failure the file reports nothing
/// more (seen_error_: upstream's writer then fails every operation before reaching the file).
/// Not thread-safe, like the files it wraps.
/// </summary>
internal sealed class NotifyingWritableFile : IWritableFile, ITruncatableWritableFile, IFileIoWriteReporter
{
    private readonly IWritableFile _inner;
    private readonly FileIoNotifier _notifier;
    private readonly string _path;
    private readonly bool _innerReportsWrites;
    private bool _seenError;
    private bool _closed;

    public NotifyingWritableFile(IWritableFile inner, FileIoNotifier notifier, string path)
    {
        _inner = inner;
        _notifier = notifier;
        _path = path;
        _innerReportsWrites = inner is IFileIoWriteReporter { ReportsFileIoWrites: true };
    }

    public bool ReportsFileIoWrites => true;

    public long Size => _inner.Size;

    /// <summary>Forwarded when the wrapped file supports it; not reported (the WAL truncation calls the file directly upstream).</summary>
    public void Truncate(long size)
    {
        if (_inner is ITruncatableWritableFile t) t.Truncate(size);
    }

    private void Report(FileOperationType type, ulong offset, ulong length, DateTime startTime, long start, Exception? failure) =>
        _notifier.Finish(type, _path, Temperature.Unknown, offset, length, startTime, start, failure);

    // WriteBuffered: "NotifyOnFileWriteFinish(old_size, allowed, ...); if (!s.ok()) NotifyOnIOError(s, kAppend, file_name(),
    // allowed, old_size)".
    private void WriteFailed(Exception e, ulong offset, ulong length, DateTime startTime, long start)
    {
        _seenError = true;
        Report(FileOperationType.Write, offset, length, startTime, start, e);
        _notifier.IoError(e, FileOperationType.Append, _path, length, offset);
    }

    public void Append(ReadOnlySpan<byte> data)
    {
        if (_innerReportsWrites || _seenError)
        {
            AppendUnreported(data);
            return;
        }
        ulong offset = (ulong)_inner.Size;
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            _inner.Append(data);
        }
        catch (Exception e)
        {
            WriteFailed(e, offset, (ulong)data.Length, startTime, start);
            throw;
        }
        Report(FileOperationType.Write, offset, (ulong)data.Length, startTime, start, null);
    }

    private void AppendUnreported(ReadOnlySpan<byte> data)
    {
        try
        {
            _inner.Append(data);
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (_innerReportsWrites || _seenError) return Guard(_inner.AppendAsync(data, cancellationToken));
        return AppendReportedAsync(data, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AppendReportedAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        ulong offset = (ulong)_inner.Size;
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            await _inner.AppendAsync(data, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            WriteFailed(e, offset, (ulong)data.Length, startTime, start);
            throw;
        }
        Report(FileOperationType.Write, offset, (ulong)data.Length, startTime, start, null);
    }

    /// <summary>Records a failure of an unreported operation (seen_error_).</summary>
    private ValueTask Guard(ValueTask t) => t.IsCompletedSuccessfully ? t : GuardAwaited(t);

    private async ValueTask GuardAwaited(ValueTask t)
    {
        try
        {
            await t.ConfigureAwait(false);
        }
        catch
        {
            _seenError = true;
            throw;
        }
    }

    // WritableFileWriter::Flush: the buffered data is written (reported as writes), then "NotifyOnFileFlushFinish(start_ts,
    // finish_ts, s); if (!s.ok()) NotifyOnIOError(s, kFlush, file_name())". When the wrapped file reports its writes a
    // failure of its flush is a failed write, already reported (upstream returns before the flush notification).
    private void FlushFailed(Exception e, DateTime startTime, long start)
    {
        _seenError = true;
        if (_innerReportsWrites) return;
        Report(FileOperationType.Flush, 0, 0, startTime, start, e);
        _notifier.IoError(e, FileOperationType.Flush, _path, 0, 0);
    }

    public void Flush()
    {
        if (_seenError)
        {
            _inner.Flush();
            return;
        }
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            _inner.Flush();
        }
        catch (Exception e)
        {
            FlushFailed(e, startTime, start);
            throw;
        }
        Report(FileOperationType.Flush, 0, 0, startTime, start, null);
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
        _seenError ? _inner.FlushAsync(cancellationToken) : FlushReportedAsync(cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask FlushReportedAsync(CancellationToken cancellationToken)
    {
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            await _inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            FlushFailed(e, startTime, start);
            throw;
        }
        Report(FileOperationType.Flush, 0, 0, startTime, start, null);
    }

    // WritableFileWriter::SyncInternal: "NotifyOnFileSyncFinish(start_ts, finish_ts, s, kSync); if (!s.ok())
    // NotifyOnIOError(s, kSync, file_name())" (the file systems here sync data: fdatasync, kSync).
    private void SyncFailed(Exception e, DateTime startTime, long start)
    {
        _seenError = true;
        Report(FileOperationType.Sync, 0, 0, startTime, start, e);
        _notifier.IoError(e, FileOperationType.Sync, _path, 0, 0);
    }

    /// <summary>WritableFileWriter::Sync: Flush (reported), then the sync.</summary>
    public void Sync()
    {
        if (_seenError)
        {
            _inner.Sync();
            return;
        }
        Flush();
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            _inner.Sync();
        }
        catch (Exception e)
        {
            SyncFailed(e, startTime, start);
            throw;
        }
        Report(FileOperationType.Sync, 0, 0, startTime, start, null);
    }

    public ValueTask SyncAsync(CancellationToken cancellationToken = default) =>
        _seenError ? _inner.SyncAsync(cancellationToken) : SyncReportedAsync(cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SyncReportedAsync(CancellationToken cancellationToken)
    {
        await FlushReportedAsync(cancellationToken).ConfigureAwait(false);
        long start = FileIoNotifier.Start(out DateTime startTime);
        try
        {
            await _inner.SyncAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            SyncFailed(e, startTime, start);
            throw;
        }
        Report(FileOperationType.Sync, 0, 0, startTime, start, null);
    }

    /// <summary>
    /// WritableFileWriter::Close: a writer that saw an error closes the file without notifications; otherwise Flush
    /// (reported), the close, "NotifyOnFileCloseFinish(start_ts, finish_ts, s)" with the status s of the flush, and
    /// "NotifyOnIOError(interim, kClose, file_name())" when the close failed.
    /// </summary>
    public void Dispose()
    {
        if (_closed) return;
        _closed = true;
        if (_seenError)
        {
            _inner.Dispose();
            return;
        }
        Exception? flushFailure = null;
        try
        {
            Flush();
        }
        catch (Exception e)
        {
            flushFailure = e;
        }
        long start = FileIoNotifier.Start(out DateTime startTime);
        Exception? closeFailure = null;
        try
        {
            _inner.Dispose();
        }
        catch (Exception e)
        {
            closeFailure = e;
        }
        Closed(flushFailure, closeFailure, startTime, start);
    }

    public async ValueTask DisposeAsync()
    {
        if (_closed) return;
        _closed = true;
        if (_seenError)
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
            return;
        }
        Exception? flushFailure = null;
        try
        {
            await FlushReportedAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            flushFailure = e;
        }
        long start = FileIoNotifier.Start(out DateTime startTime);
        Exception? closeFailure = null;
        try
        {
            await _inner.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            closeFailure = e;
        }
        Closed(flushFailure, closeFailure, startTime, start);
    }

    private void Closed(Exception? flushFailure, Exception? closeFailure, DateTime startTime, long start)
    {
        Report(FileOperationType.Close, 0, 0, startTime, start, flushFailure);
        if (closeFailure is not null) _notifier.IoError(closeFailure, FileOperationType.Close, _path, 0, 0);
        // The flush's failure first (upstream's Close returns it), else the close's.
        Exception? failure = flushFailure ?? closeFailure;
        if (failure is not null) ExceptionDispatchInfo.Capture(failure).Throw();
    }
}
