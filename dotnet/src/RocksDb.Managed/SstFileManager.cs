using System.Globalization;
using RocksDb.Managed.Db;
using RocksDb.Managed.Env;

namespace RocksDb.Managed;

/// <summary>
/// Tracks the SST and blob files of one or more DBs, enforces a space limit, rate limits file deletions and drives the
/// automatic recovery from out-of-space errors. Port of SstFileManagerImpl (file/sst_file_manager_impl.cc,
/// include/rocksdb/sst_file_manager.h) and DeleteScheduler (file/delete_scheduler.cc).
/// <para>
/// Set it as <see cref="DbOptions.SstFileManager"/>. The DB reports every table and blob file it creates or deletes.
/// <list type="bullet">
/// <item><b>Space limit</b> (<see cref="SetMaxAllowedSpaceUsage"/>): when the tracked files reach it after a flush or
/// compaction the DB stops with a SpaceLimit background error ("Max allowed space was reached"); compactions that would
/// not fit (inputs + <see cref="SetCompactionBufferSize">compaction buffer</see>) are not started.</item>
/// <item><b>Deletion rate</b> (<see cref="DeleteRateBytesPerSecond"/>, 0 = delete immediately): obsolete files are renamed
/// to <c>*.trash</c> and deleted by a background task at that rate, unless the trash exceeds
/// <see cref="MaxTrashDbRatio"/> of the tracked size (then they are deleted right away). Trash left over by an earlier
/// process is deleted when a DB opens.</item>
/// <item><b>Out-of-space recovery</b>: a NoSpace / SpaceLimit background error is recovered automatically once enough
/// space is free (polled every 5 seconds, ClearError), when the file system reports free space
/// (<see cref="IFileSystem.GetFreeSpace"/>).</item>
/// </list>
/// Thread-safe. Dispose stops the background tasks.
/// </para>
/// </summary>
public sealed class SstFileManager : IDisposable
{
    private readonly IFileSystem _fs;
    private readonly Lock _lock = new();
    private readonly Dictionary<string, ulong> _trackedFiles = new(StringComparer.Ordinal);
    private ulong _totalFilesSize;
    private ulong _compactionBufferSize;
    private ulong _curCompactionsReservedSize;
    private ulong _maxAllowedSpace;
    private ulong _reservedDiskBuffer;
    private ulong _freeSpaceTrigger;
    private string _path = "";
    private bool _closing;
    // Error recovery (ClearError): the DBs waiting for space, the error severity to clear.
    private readonly LinkedList<ErrorHandler> _errorHandlers = new();
    private ErrorHandler? _currentInstance;
    private RocksDbException? _bgError;
    private Task _recoveryTask = Task.CompletedTask;
    private readonly CancellationTokenSource _closeCancellation = new();
    private readonly DeleteScheduler _deleteScheduler;

    /// <summary>NewSstFileManager with the defaults: local file system, no deletion rate limit, max trash ratio 0.25, 64 MiB delete chunk.</summary>
    public SstFileManager() : this(LocalFileSystem.Instance, 0, 0.25, 64UL << 20)
    {
    }

    /// <param name="fileSystem">File system of the DB files.</param>
    /// <param name="rateBytesPerSecond">Deletion rate; 0 deletes files immediately.</param>
    /// <param name="maxTrashDbRatio">Trash larger than this fraction of the tracked size is deleted immediately.</param>
    /// <param name="bytesMaxDeleteChunk">Kept for API parity: files are always deleted whole (no ftruncate chunking).</param>
    public SstFileManager(IFileSystem fileSystem, long rateBytesPerSecond, double maxTrashDbRatio, ulong bytesMaxDeleteChunk)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        if (maxTrashDbRatio < 0) throw new ArgumentOutOfRangeException(nameof(maxTrashDbRatio));
        _fs = fileSystem;
        BytesMaxDeleteChunk = bytesMaxDeleteChunk;
        _deleteScheduler = new DeleteScheduler(this, fileSystem, rateBytesPerSecond, maxTrashDbRatio);
    }

    /// <summary>The file system this manager deletes and measures files with.</summary>
    public IFileSystem FileSystem => _fs;

    /// <summary>bytes_max_delete_chunk (not used: the managed port deletes trash files whole).</summary>
    public ulong BytesMaxDeleteChunk { get; }

    /// <summary>Interval between free-space checks while recovering from an out-of-space error (5 s upstream).</summary>
    internal TimeSpan ErrorRecoveryPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    // ---- space accounting -----------------------------------------------------------------------

    /// <summary>SetMaxAllowedSpaceUsage: 0 disables the limit.</summary>
    public void SetMaxAllowedSpaceUsage(ulong maxAllowedSpace)
    {
        lock (_lock) _maxAllowedSpace = maxAllowedSpace;
    }

    /// <summary>SetCompactionBufferSize: free space a compaction must leave besides its inputs.</summary>
    public void SetCompactionBufferSize(ulong compactionBufferSize)
    {
        lock (_lock) _compactionBufferSize = compactionBufferSize;
    }

    /// <summary>IsMaxAllowedSpaceReached.</summary>
    public bool IsMaxAllowedSpaceReached
    {
        get
        {
            lock (_lock) return _maxAllowedSpace > 0 && _totalFilesSize >= _maxAllowedSpace;
        }
    }

    /// <summary>IsMaxAllowedSpaceReachedIncludingCompactions.</summary>
    public bool IsMaxAllowedSpaceReachedIncludingCompactions
    {
        get
        {
            lock (_lock) return _maxAllowedSpace > 0 && _totalFilesSize + _curCompactionsReservedSize >= _maxAllowedSpace;
        }
    }

    /// <summary>GetTotalSize: total size of the tracked files.</summary>
    public ulong TotalSize
    {
        get
        {
            lock (_lock) return _totalFilesSize;
        }
    }

    /// <summary>GetCompactionsReservedSize.</summary>
    public ulong CompactionsReservedSize
    {
        get
        {
            lock (_lock) return _curCompactionsReservedSize;
        }
    }

    /// <summary>GetTrackedFiles: path -> size.</summary>
    public Dictionary<string, ulong> GetTrackedFiles()
    {
        lock (_lock) return new Dictionary<string, ulong>(_trackedFiles, StringComparer.Ordinal);
    }

    /// <summary>Get/SetDeleteRateBytesPerSecond (0 = immediate deletion).</summary>
    public long DeleteRateBytesPerSecond
    {
        get => _deleteScheduler.RateBytesPerSecond;
        set => _deleteScheduler.RateBytesPerSecond = value;
    }

    /// <summary>Get/SetMaxTrashDBRatio.</summary>
    public double MaxTrashDbRatio
    {
        get => _deleteScheduler.MaxTrashDbRatio;
        set
        {
            if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));
            _deleteScheduler.MaxTrashDbRatio = value;
        }
    }

    /// <summary>GetTotalTrashSize: bytes waiting in trash files.</summary>
    public ulong TotalTrashSize => _deleteScheduler.TotalTrashSize;

    /// <summary>WaitForEmptyTrash: blocks until the deletion queue is empty.</summary>
    public void WaitForEmptyTrash() => _deleteScheduler.WaitForEmptyTrash();

    internal void OnAddFile(string path)
    {
        long size;
        try { size = _fs.GetFileSize(path); }
        catch (Exception e) when (e is IOException or RocksDbException) { return; }
        OnAddFile(path, (ulong)size);
    }

    internal void OnAddFile(string path, ulong size)
    {
        lock (_lock) OnAddFileLocked(path, size);
    }

    internal void OnDeleteFile(string path)
    {
        lock (_lock) OnDeleteFileLocked(path);
    }

    /// <summary>OnMoveFile: the tracked size moves to the new name. Returns the size.</summary>
    internal ulong OnMoveFile(string oldPath, string newPath)
    {
        lock (_lock)
        {
            ulong size = _trackedFiles.GetValueOrDefault(oldPath);
            OnAddFileLocked(newPath, size);
            OnDeleteFileLocked(oldPath);
            return size;
        }
    }

    private void OnAddFileLocked(string path, ulong size)
    {
        if (_trackedFiles.TryGetValue(path, out ulong old)) _totalFilesSize -= old;
        _trackedFiles[path] = size;
        _totalFilesSize += size;
        // The compaction that produced the file no longer needs its reservation for it.
    }

    private void OnDeleteFileLocked(string path)
    {
        if (!_trackedFiles.Remove(path, out ulong size)) return;
        _totalFilesSize -= size;
    }

    /// <summary>
    /// EnoughRoomForCompaction: reserves room for a compaction of <paramref name="inputSize"/> bytes of input. False when
    /// the space limit (or, after an out-of-space error, the free space) does not allow it.
    /// </summary>
    internal bool EnoughRoomForCompaction(ulong inputSize, string pathForFreeSpace, RocksDbException? bgError)
    {
        lock (_lock)
        {
            ulong neededHeadroom = _curCompactionsReservedSize + inputSize + _compactionBufferSize;
            if (_maxAllowedSpace != 0 && neededHeadroom + _totalFilesSize > _maxAllowedSpace) return false;
            if (bgError?.SubCode == StatusSubCode.NoSpace)
            {
                ulong freeSpace;
                try { freeSpace = (ulong)_fs.GetFreeSpace(pathForFreeSpace); }
                catch (RocksDbException) { freeSpace = ulong.MaxValue; }
                if (_compactionBufferSize == 0) neededHeadroom += _reservedDiskBuffer;
                if (freeSpace < neededHeadroom + inputSize) return false;
            }
            _curCompactionsReservedSize += inputSize;
            _freeSpaceTrigger = _curCompactionsReservedSize;
            return true;
        }
    }

    /// <summary>OnCompactionCompletion: releases the reservation of a compaction.</summary>
    internal void OnCompactionCompletion(ulong inputSize)
    {
        lock (_lock) _curCompactionsReservedSize -= Math.Min(inputSize, _curCompactionsReservedSize);
    }

    /// <summary>ReserveDiskBuffer: free space needed to clear a hard out-of-space error (the DB's memtable budget).</summary>
    internal void ReserveDiskBuffer(ulong size, string path)
    {
        lock (_lock)
        {
            _reservedDiskBuffer += size;
            if (_path.Length == 0) _path = path;
        }
    }

    // ---- deletion -------------------------------------------------------------------------------

    /// <summary>
    /// ScheduleFileDeletion: deletes a tracked file, through the trash queue when a deletion rate is set (and
    /// <paramref name="forceBackground"/> or the trash is small enough), else immediately.
    /// </summary>
    internal void ScheduleFileDeletion(string path, string directoryToSync, bool forceBackground) =>
        _deleteScheduler.DeleteFile(path, directoryToSync, forceBackground);

    /// <summary>DeleteScheduler::CleanupDirectory: schedules the deletion of trash left in <paramref name="directory"/>.</summary>
    internal void CleanupTrash(string directory)
    {
        IEnumerable<string> children;
        try { children = _fs.GetChildren(directory); }
        catch (Exception e) when (e is IOException or RocksDbException) { return; }
        foreach (string child in children)
        {
            if (!DeleteScheduler.IsTrashFile(child)) continue;
            string trash = directory + "/" + child;
            OnAddFile(trash);
            ScheduleFileDeletion(trash, directory, forceBackground: false);
        }
    }

    // ---- out-of-space recovery (StartErrorRecovery / ClearError / CancelErrorRecovery) ------------

    /// <summary>StartErrorRecovery: a DB stopped by an out-of-space error waits for free space.</summary>
    internal void StartErrorRecovery(ErrorHandler handler, RocksDbException bgError)
    {
        lock (_lock)
        {
            if (bgError.Severity == ErrorSeverity.SoftError)
            {
                _bgError ??= bgError;
            }
            else
            {
                _bgError = bgError;
            }
            if (_errorHandlers.Contains(handler)) return;
            _errorHandlers.AddLast(handler);
            if (_errorHandlers.Count == 1 && !_closing)
            {
                Task previous = _recoveryTask;
                _recoveryTask = Task.Run(async () =>
                {
                    await previous.ConfigureAwait(false);
                    await ClearErrorAsync().ConfigureAwait(false);
                });
            }
        }
    }

    /// <summary>CancelErrorRecovery: false when the recovery of <paramref name="handler"/> is running right now.</summary>
    internal bool CancelErrorRecovery(ErrorHandler handler)
    {
        lock (_lock)
        {
            if (_currentInstance == handler)
            {
                _currentInstance = null;
                return false;
            }
            return _errorHandlers.Remove(handler);
        }
    }

    /// <summary>ClearError: polls the free space and recovers the waiting DBs one at a time.</summary>
    private async Task ClearErrorAsync()
    {
        while (true)
        {
            ErrorHandler? handler = null;
            RocksDbException? s = null;
            lock (_lock)
            {
                if (_errorHandlers.Count == 0 || _closing) return;
                ulong freeSpace = ulong.MaxValue;
                bool known = true;
                try { freeSpace = (ulong)_fs.GetFreeSpace(_path.Length != 0 ? _path : "/"); }
                catch (Exception e) when (e is IOException or RocksDbException) { known = false; }
                if (_maxAllowedSpace > 0) freeSpace = Math.Min(_maxAllowedSpace, freeSpace);
                if (known)
                {
                    if (_bgError?.Severity == ErrorSeverity.HardError && freeSpace < _reservedDiskBuffer) s = NoSpace();
                    else if (_bgError?.Severity == ErrorSeverity.SoftError && freeSpace < _freeSpaceTrigger) s = NoSpace();
                }
                if (s is null)
                {
                    handler = _errorHandlers.First!.Value;
                    _currentInstance = handler;
                }
            }
            if (handler is not null)
            {
                try
                {
                    s = await handler.RecoverFromBgErrorAsync(isManual: false).ConfigureAwait(false);
                }
                catch (Exception e) when (e is RocksDbException or IOException or ObjectDisposedException)
                {
                    s = RocksDbException.FromException(e);
                }
                // Since we're past the recovery, we can look at this error. Read it before taking the lock: it takes the
                // DB mutex, and the DB calls into this manager (OnAddFile from VersionSet.InstallEdit) while holding the
                // DB mutex, so reading it under the lock deadlocked with a concurrent flush or compaction.
                RocksDbException? err = handler.CurrentBgError;
                lock (_lock)
                {
                    if (_currentInstance is not null)
                    {
                        if (s is null && err is not null && err.SubCode == StatusSubCode.NoSpace && err.Severity < ErrorSeverity.FatalError) s = err;
                        _currentInstance = null;
                    }
                    if (s is null || s.Code == StatusCode.ShutdownInProgress || s.Severity >= ErrorSeverity.FatalError)
                    {
                        _errorHandlers.Remove(handler);
                    }
                }
            }
            bool empty;
            lock (_lock) empty = _errorHandlers.Count == 0;
            if (empty)
            {
                lock (_lock) _bgError = null;
                return;
            }
            try
            {
                await Task.Delay(ErrorRecoveryPollInterval, _closeCancellation.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private static RocksDbException NoSpace() => RocksDbException.NoSpace("not enough free space to clear the background error");

    /// <summary>Close: stops the recovery and trash deletion tasks.</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            if (_closing) return;
            _closing = true;
        }
        _closeCancellation.Cancel();
        _deleteScheduler.Dispose();
        try { _recoveryTask.Wait(TimeSpan.FromSeconds(30)); }
        catch (AggregateException) { }
    }

    /// <summary>
    /// Port of DeleteScheduler (file/delete_scheduler.cc): renames obsolete files to <c>*.trash</c> and deletes them on a
    /// background task, waiting after each file so that the deleted bytes never exceed the rate.
    /// </summary>
    private sealed class DeleteScheduler : IDisposable
    {
        public const string TrashExtension = ".trash";

        private readonly SstFileManager _sfm;
        private readonly IFileSystem _fs;
        private readonly Lock _lock = new();
        private readonly Queue<(string TrashPath, string Dir, ulong Size)> _queue = new();
        private readonly SemaphoreSlim _signal = new(0);
        private readonly CancellationTokenSource _close = new();
        private readonly Task _worker;
        private long _rateBytesPerSecond;
        private double _maxTrashDbRatio;
        private long _totalTrashSize;
        private int _pending;
        private TaskCompletionSource _empty = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public DeleteScheduler(SstFileManager sfm, IFileSystem fs, long rateBytesPerSecond, double maxTrashDbRatio)
        {
            _sfm = sfm;
            _fs = fs;
            _rateBytesPerSecond = rateBytesPerSecond;
            _maxTrashDbRatio = maxTrashDbRatio;
            _empty.TrySetResult();
            _worker = Task.Run(BackgroundEmptyTrashAsync);
        }

        public long RateBytesPerSecond
        {
            get => Interlocked.Read(ref _rateBytesPerSecond);
            set => Interlocked.Exchange(ref _rateBytesPerSecond, value);
        }

        public double MaxTrashDbRatio
        {
            get => Volatile.Read(ref _maxTrashDbRatio);
            set => Volatile.Write(ref _maxTrashDbRatio, value);
        }

        public ulong TotalTrashSize => (ulong)Interlocked.Read(ref _totalTrashSize);

        public static bool IsTrashFile(string path) => path.EndsWith(TrashExtension, StringComparison.Ordinal);

        /// <summary>DeleteScheduler::DeleteFile.</summary>
        public void DeleteFile(string path, string dirToSync, bool forceBackground)
        {
            ulong totalSize = _sfm.TotalSize;
            if (!IsTrashFile(path) && (RateBytesPerSecond <= 0 ||
                                       (!forceBackground && TotalTrashSize > totalSize * MaxTrashDbRatio)))
            {
                DeleteFileImmediately(path);
                return;
            }
            // MarkAsTrash: rename to a unique *.trash name.
            string trash = path;
            if (!IsTrashFile(path))
            {
                trash = path + TrashExtension;
                for (int cnt = 0; _fs.FileExists(trash); cnt++) trash = path + cnt.ToString(CultureInfo.InvariantCulture) + TrashExtension;
                try
                {
                    _fs.RenameFile(path, trash);
                }
                catch (Exception e) when (e is IOException or RocksDbException)
                {
                    DeleteFileImmediately(path);
                    return;
                }
                _sfm.OnMoveFile(path, trash);
            }
            ulong size = 0;
            try { size = (ulong)_fs.GetFileSize(trash); }
            catch (Exception e) when (e is IOException or RocksDbException) { }
            Interlocked.Add(ref _totalTrashSize, (long)size);
            lock (_lock)
            {
                _queue.Enqueue((trash, dirToSync, size));
                if (_pending++ == 0) _empty = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            _signal.Release();
        }

        private void DeleteFileImmediately(string path)
        {
            try
            {
                _fs.DeleteFile(path);
            }
            catch (Exception e) when (e is IOException or RocksDbException)
            {
                if (_fs.FileExists(path)) return; // keep tracking a file that could not be deleted
            }
            _sfm.OnDeleteFile(path);
        }

        /// <summary>BackgroundEmptyTrash: deletes queued trash at the configured rate.</summary>
        private async Task BackgroundEmptyTrashAsync()
        {
            CancellationToken ct = _close.Token;
            while (!ct.IsCancellationRequested)
            {
                try { await _signal.WaitAsync(ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
                long start = System.Diagnostics.Stopwatch.GetTimestamp();
                ulong totalDeleted = 0;
                long rate = RateBytesPerSecond;
                while (true)
                {
                    (string TrashPath, string Dir, ulong Size) item;
                    lock (_lock)
                    {
                        if (!_queue.TryDequeue(out item)) break;
                    }
                    if (rate != RateBytesPerSecond)
                    {
                        rate = RateBytesPerSecond;
                        start = System.Diagnostics.Stopwatch.GetTimestamp();
                        totalDeleted = 0;
                    }
                    try
                    {
                        _fs.DeleteFile(item.TrashPath);
                        if (item.Dir.Length != 0) _fs.SyncDirectory(item.Dir);
                    }
                    catch (Exception e) when (e is IOException or RocksDbException)
                    {
                    }
                    _sfm.OnDeleteFile(item.TrashPath);
                    Interlocked.Add(ref _totalTrashSize, -(long)item.Size);
                    totalDeleted += item.Size;
                    if (rate > 0)
                    {
                        // Penalty: the deleted bytes may not exceed the rate since the batch started.
                        TimeSpan due = TimeSpan.FromSeconds((double)totalDeleted / rate) - System.Diagnostics.Stopwatch.GetElapsedTime(start);
                        if (due > TimeSpan.Zero)
                        {
                            try { await Task.Delay(due, ct).ConfigureAwait(false); }
                            catch (OperationCanceledException) { return; }
                        }
                    }
                    lock (_lock)
                    {
                        if (--_pending == 0) _empty.TrySetResult();
                    }
                    if (_signal.CurrentCount > 0) _signal.Wait(0);
                }
            }
        }

        public void WaitForEmptyTrash()
        {
            Task t;
            lock (_lock) t = _empty.Task;
            t.Wait();
        }

        public void Dispose()
        {
            _close.Cancel();
            try { _worker.Wait(TimeSpan.FromSeconds(30)); }
            catch (AggregateException) { }
            lock (_lock) _empty.TrySetResult();
        }
    }
}
