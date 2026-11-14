using RocksDb.Managed.Env;

namespace RocksDb.Managed.Tests.Db;

/// <summary>Kind of error a <see cref="FaultInjectionFileSystem"/> injects (the IOStatus flavours of fault_injection_fs).</summary>
internal enum InjectedFaultKind
{
    /// <summary>Plain IOError (not retryable).</summary>
    IOError,
    /// <summary>IOError with the retryable flag (IOStatus::SetRetryable(true)).</summary>
    Retryable,
    /// <summary>IOError with the file scope (treated as retryable by the write path).</summary>
    FileScope,
    /// <summary>IOStatus::NoSpace.</summary>
    NoSpace,
    /// <summary>IOError with the data loss flag.</summary>
    DataLoss,
}

/// <summary>How much of its data a failing append (<see cref="FaultInjectionFileSystem.FailAppend"/>) writes first.</summary>
internal enum AppendFailMode
{
    /// <summary>Nothing reaches the file.</summary>
    NothingWritten,
    /// <summary>The first half of the data reaches the file (a torn write).</summary>
    Torn,
    /// <summary>All of the data reaches the file, then the error is reported (an ambiguous outcome).</summary>
    FullyWritten,
}

/// <summary>Operations a targeted fault applies to.</summary>
[Flags]
internal enum FaultOps
{
    None = 0,
    Create = 1,
    Append = 2,
    Sync = 4,
    Rename = 8,
    SyncDirectory = 16,
    All = Create | Append | Sync | Rename | SyncDirectory,
}

/// <summary>
/// Test wrapper over <see cref="MemoryFileSystem"/> (in the spirit of utilities/fault_injection_fs.cc):
/// <list type="bullet">
/// <item>Counts mutating operations (create, append, flush, sync, delete, rename, link, directory sync). When
/// <see cref="CrashAtOperation"/> is reached that operation fails and the file system becomes inactive: every later
/// mutation fails too, like a process that died at that point. With <see cref="TornWrites"/> the failing append
/// still writes the first half of its data (a torn record).</item>
/// <item>Tracks open file handles so tests can assert that nothing leaks after the DB is closed.</item>
/// </list>
/// After the "crash", call <see cref="PowerLoss"/> (unsynced data and directory entries are lost) or
/// <see cref="ProcessKill"/> (everything written so far survives), then <see cref="Restart"/>.
/// </summary>
internal sealed class FaultInjectionFileSystem : IFileSystem
{
    private long _ops;
    private int _openFiles;
    private int _openLocks;

    public FaultInjectionFileSystem(MemoryFileSystem inner) => Inner = inner;

    public MemoryFileSystem Inner { get; }

    /// <summary>1-based index of the mutating operation that fails (0 = never).</summary>
    public long CrashAtOperation { get; set; }

    /// <summary>The failing append writes the first half of its data before failing.</summary>
    public bool TornWrites { get; set; }

    /// <summary>Only the operation at <see cref="CrashAtOperation"/> fails; the file system stays usable afterwards.</summary>
    public bool TransientFailure { get; set; }

    /// <summary>
    /// One-shot: the next append to a file whose name contains this fragment fails (torn if <see cref="TornWrites"/>);
    /// the file system stays usable.
    /// </summary>
    public string? FailNextAppendTo { get; set; }

    /// <summary>Kind of error raised by every injected fault.</summary>
    public InjectedFaultKind ErrorKind { get; set; } = InjectedFaultKind.IOError;

    private readonly Lock _targetLock = new();
    private string? _targetFragment;
    private FaultOps _targetOps;
    private int _targetRemaining;
    private InjectedFaultKind _targetKind;
    private int _targetHits;

    /// <summary>
    /// The next <paramref name="count"/> operations of kind <paramref name="ops"/> on paths containing
    /// <paramref name="fragment"/> fail with <paramref name="kind"/> (the file system stays usable).
    /// </summary>
    public void FailMatching(string fragment, FaultOps ops, int count, InjectedFaultKind kind)
    {
        lock (_targetLock)
        {
            _targetFragment = fragment;
            _targetOps = ops;
            _targetRemaining = count;
            _targetKind = kind;
        }
    }

    /// <summary>Stops <see cref="FailMatching"/> faults.</summary>
    public void ClearFailMatching()
    {
        lock (_targetLock) _targetRemaining = 0;
    }

    private string? _appendFragment;
    private byte[]? _appendMarker;
    private AppendFailMode _appendMode;
    private InjectedFaultKind _appendKind;

    /// <summary>
    /// One-shot: the next append to a path containing <paramref name="fragment"/> whose data contains
    /// <paramref name="marker"/> (any append when null) writes what <paramref name="mode"/> says, then fails with
    /// <paramref name="kind"/>. The file system stays usable. Counted in <see cref="TargetedFaults"/>.
    /// </summary>
    public void FailAppend(string fragment, byte[]? marker, AppendFailMode mode, InjectedFaultKind kind)
    {
        lock (_targetLock)
        {
            _appendFragment = fragment;
            _appendMarker = marker;
            _appendMode = mode;
            _appendKind = kind;
        }
    }

    /// <summary>Applies a pending <see cref="FailAppend"/> fault to this append, if it matches.</summary>
    private void CheckFailAppend(string path, ReadOnlySpan<byte> data, IWritableFile inner)
    {
        AppendFailMode mode;
        InjectedFaultKind kind;
        lock (_targetLock)
        {
            if (_appendFragment is null || !path.Contains(_appendFragment, StringComparison.Ordinal)) return;
            if (_appendMarker is not null && data.IndexOf(_appendMarker) < 0) return;
            _appendFragment = null;
            mode = _appendMode;
            kind = _appendKind;
        }
        Interlocked.Increment(ref _targetHits);
        if (mode == AppendFailMode.FullyWritten) inner.Append(data);
        else if (mode == AppendFailMode.Torn && data.Length > 1) inner.Append(data[..(data.Length / 2)]);
        throw MakeError(kind);
    }

    /// <summary>Number of faults raised by <see cref="FailMatching"/> and <see cref="FailAppend"/>.</summary>
    public int TargetedFaults => Volatile.Read(ref _targetHits);

    private void CheckTargeted(string path, FaultOps op)
    {
        InjectedFaultKind kind;
        lock (_targetLock)
        {
            if (_targetRemaining <= 0 || (_targetOps & op) == 0 || _targetFragment is null || !path.Contains(_targetFragment, StringComparison.Ordinal)) return;
            _targetRemaining--;
            kind = _targetKind;
        }
        Interlocked.Increment(ref _targetHits);
        throw MakeError(kind);
    }

    private static RocksDbException MakeError(InjectedFaultKind kind) => kind switch
    {
        InjectedFaultKind.Retryable => RocksDbException.RetryableIOError("injected retryable fault"),
        InjectedFaultKind.FileScope => new RocksDbException(StatusCode.IOError, "IO error: injected file-scope fault") { Scope = IOErrorScope.File },
        InjectedFaultKind.NoSpace => RocksDbException.NoSpace("injected fault"),
        InjectedFaultKind.DataLoss => new RocksDbException(StatusCode.IOError, "IO error: injected data loss") { DataLoss = true },
        _ => new RocksDbException(StatusCode.IOError, "IO error: injected fault"),
    };

    public long Operations => Interlocked.Read(ref _ops);
    public int OpenFiles => Volatile.Read(ref _openFiles);
    public int OpenLocks => Volatile.Read(ref _openLocks);
    public bool Crashed { get; private set; }

    /// <summary>Simulated power loss: unsynced data and unsynced directory entries are dropped.</summary>
    public void PowerLoss() => Inner.SimulateCrash();

    /// <summary>
    /// Simulated process kill: the OS keeps everything written so far, nothing to drop (the DB object must have been
    /// disposed, which releases its LOCK).
    /// </summary>
    public static void ProcessKill()
    {
    }

    /// <summary>Re-enables the file system after a crash (a new process).</summary>
    public void Restart()
    {
        CrashAtOperation = 0;
        Crashed = false;
        CrashedAt = null;
        Inner.SetFilesystemActive(true);
    }

    /// <summary>
    /// When set, every counted mutating operation is recorded as "op path" (in counting order, so the operation
    /// <see cref="CrashAtOperation"/> = n stopped at is the n-th entry when recording started at operation 0).
    /// </summary>
    public System.Collections.Concurrent.ConcurrentQueue<string>? OpLog { get; set; }

    /// <summary>The operation ("op path") at which the crash of <see cref="CrashAtOperation"/> happened, or null.</summary>
    public string? CrashedAt { get; private set; }

    /// <summary>Counts one mutating operation; returns true if it is the one that must fail.</summary>
    private bool Tick(string op, string path)
    {
        OpLog?.Enqueue(op + " " + path);
        long n = Interlocked.Increment(ref _ops);
        long crashAt = CrashAtOperation;
        if (crashAt > 0 && n >= crashAt && !Crashed)
        {
            Crashed = true;
            CrashedAt = op + " " + path;
            if (!TransientFailure) Inner.SetFilesystemActive(false);
            return true;
        }
        return false;
    }

    private RocksDbException InjectedError() => MakeError(ErrorKind);

    private void Mutate(string op, string path)
    {
        if (Tick(op, path)) throw InjectedError();
        Inner.ThrowIfInactive();
    }

    public IWritableFile CreateWritable(string path, bool truncate = true) => CreateWritable(path, truncate, WritableFileOptions.Default);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
    {
        CheckTargeted(path, FaultOps.Create);
        Mutate("create", path);
        IWritableFile f = Inner.CreateWritable(path, truncate, options);
        Interlocked.Increment(ref _openFiles);
        return new Writable(this, f, path);
    }

    public IWritableFile ReopenWritable(string path, WritableFileOptions options)
    {
        Mutate("reopen", path);
        IWritableFile f = Inner.ReopenWritable(path, options);
        Interlocked.Increment(ref _openFiles);
        return new Writable(this, f, path);
    }

    public IRandomAccessFile OpenRandomAccess(string path) => OpenRandomAccess(path, RandomAccessFileOptions.Default);

    /// <summary>Called with the path before every random access open (it may throw to fail the open).</summary>
    public Action<string>? BeforeOpenRandomAccess { get; set; }

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options)
    {
        BeforeOpenRandomAccess?.Invoke(path);
        IRandomAccessFile f = Inner.OpenRandomAccess(path, options);
        Interlocked.Increment(ref _openFiles);
        return new RandomAccess(this, f);
    }

    public ISequentialFile OpenSequential(string path) => OpenSequential(path, SequentialFileOptions.Default);

    public ISequentialFile OpenSequential(string path, SequentialFileOptions options)
    {
        ISequentialFile f = Inner.OpenSequential(path, options);
        Interlocked.Increment(ref _openFiles);
        return new Sequential(this, f);
    }

    public bool FileExists(string path) => Inner.FileExists(path);
    public bool DirectoryExists(string path) => Inner.DirectoryExists(path);
    public long GetFileSize(string path) => Inner.GetFileSize(path);
    public IEnumerable<string> GetChildren(string directory) => Inner.GetChildren(directory);
    public DateTimeOffset GetFileModificationTime(string path) => Inner.GetFileModificationTime(path);

    public long GetFreeSpace(string path) => Inner.GetFreeSpace(path);

    public void DeleteFile(string path)
    {
        Mutate("delete", path);
        Inner.DeleteFile(path);
    }

    /// <summary>Every successful rename (source, target), in order (WAL reuse renames the recycled WAL).</summary>
    public System.Collections.Concurrent.ConcurrentQueue<(string Source, string Target)> Renames { get; } = new();

    public void RenameFile(string source, string target)
    {
        CheckTargeted(target, FaultOps.Rename);
        Mutate("rename", source + " -> " + target);
        Inner.RenameFile(source, target);
        Renames.Enqueue((source, target));
    }

    public void LinkFile(string source, string target)
    {
        Mutate("link", target);
        Inner.LinkFile(source, target);
    }

    public void CreateDirectoryIfMissing(string path)
    {
        Mutate("mkdir", path);
        Inner.CreateDirectoryIfMissing(path);
    }

    public void DeleteDirectory(string path)
    {
        Mutate("rmdir", path);
        Inner.DeleteDirectory(path);
    }

    public void SyncDirectory(string path)
    {
        CheckTargeted(path, FaultOps.SyncDirectory);
        Mutate("syncdir", path);
        Inner.SyncDirectory(path);
    }

    public IDisposable LockFile(string path)
    {
        IDisposable l = Inner.LockFile(path);
        Interlocked.Increment(ref _openLocks);
        return new LockHandle(this, l);
    }

    private sealed class LockHandle(FaultInjectionFileSystem fs, IDisposable inner) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Decrement(ref fs._openLocks);
            inner.Dispose();
        }
    }

    private sealed class Writable(FaultInjectionFileSystem fs, IWritableFile inner, string path) : IWritableFile
    {
        private int _disposed;

        public long Size => inner.Size;

        public void Append(ReadOnlySpan<byte> data)
        {
            fs.CheckTargeted(path, FaultOps.Append);
            fs.CheckFailAppend(path, data, inner);
            string? fragment = fs.FailNextAppendTo;
            bool targeted = fragment is not null && path.Contains(fragment, StringComparison.Ordinal);
            if (targeted) fs.FailNextAppendTo = null;
            if (fs.Tick("append", path) || targeted)
            {
                if (fs.TornWrites && data.Length > 1)
                {
                    bool active = fs.Inner.IsFilesystemActive;
                    fs.Inner.SetFilesystemActive(true);
                    try { inner.Append(data[..(data.Length / 2)]); }
                    finally { fs.Inner.SetFilesystemActive(active); }
                }
                throw fs.InjectedError();
            }
            inner.Append(data);
        }

        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            try
            {
                Append(data.Span);
                return ValueTask.CompletedTask;
            }
            catch (Exception e)
            {
                return ValueTask.FromException(e);
            }
        }

        public void Flush()
        {
            if (fs.Tick("flush", path)) throw fs.InjectedError();
            inner.Flush();
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                Flush();
                return ValueTask.CompletedTask;
            }
            catch (Exception e)
            {
                return ValueTask.FromException(e);
            }
        }

        public void Sync()
        {
            fs.CheckTargeted(path, FaultOps.Sync);
            if (fs.Tick("sync", path)) throw fs.InjectedError();
            inner.Sync();
        }

        public ValueTask SyncAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                Sync();
                return ValueTask.CompletedTask;
            }
            catch (Exception e)
            {
                return ValueTask.FromException(e);
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Decrement(ref fs._openFiles);
            inner.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class RandomAccess(FaultInjectionFileSystem fs, IRandomAccessFile inner) : IRandomAccessFile
    {
        private int _disposed;

        public long Length => inner.Length;
        public int Read(long offset, Span<byte> destination) => inner.Read(offset, destination);

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(offset, destination, cancellationToken);

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Decrement(ref fs._openFiles);
            inner.Dispose();
        }
    }

    private sealed class Sequential(FaultInjectionFileSystem fs, ISequentialFile inner) : ISequentialFile
    {
        private int _disposed;

        public int Read(Span<byte> destination) => inner.Read(destination);
        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default) => inner.ReadAsync(destination, cancellationToken);
        public void Skip(long bytes) => inner.Skip(bytes);

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Interlocked.Decrement(ref fs._openFiles);
            inner.Dispose();
        }
    }
}
