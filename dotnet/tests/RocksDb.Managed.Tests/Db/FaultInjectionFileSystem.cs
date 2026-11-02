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

    /// <summary>Number of faults raised by <see cref="FailMatching"/>.</summary>
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
        Inner.SetFilesystemActive(true);
    }

    /// <summary>Counts one mutating operation; returns true if it is the one that must fail.</summary>
    private bool Tick()
    {
        long n = Interlocked.Increment(ref _ops);
        long crashAt = CrashAtOperation;
        if (crashAt > 0 && n >= crashAt && !Crashed)
        {
            Crashed = true;
            if (!TransientFailure) Inner.SetFilesystemActive(false);
            return true;
        }
        return false;
    }

    private RocksDbException InjectedError() => MakeError(ErrorKind);

    private void Mutate()
    {
        if (Tick()) throw InjectedError();
        Inner.ThrowIfInactive();
    }

    public IWritableFile CreateWritable(string path, bool truncate = true) => CreateWritable(path, truncate, WritableFileOptions.Default);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
    {
        CheckTargeted(path, FaultOps.Create);
        Mutate();
        IWritableFile f = Inner.CreateWritable(path, truncate, options);
        Interlocked.Increment(ref _openFiles);
        return new Writable(this, f, path);
    }

    public IWritableFile ReopenWritable(string path, WritableFileOptions options)
    {
        Mutate();
        IWritableFile f = Inner.ReopenWritable(path, options);
        Interlocked.Increment(ref _openFiles);
        return new Writable(this, f, path);
    }

    public IRandomAccessFile OpenRandomAccess(string path) => OpenRandomAccess(path, RandomAccessFileOptions.Default);

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options)
    {
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
        Mutate();
        Inner.DeleteFile(path);
    }

    public void RenameFile(string source, string target)
    {
        CheckTargeted(target, FaultOps.Rename);
        Mutate();
        Inner.RenameFile(source, target);
    }

    public void LinkFile(string source, string target)
    {
        Mutate();
        Inner.LinkFile(source, target);
    }

    public void CreateDirectoryIfMissing(string path)
    {
        Mutate();
        Inner.CreateDirectoryIfMissing(path);
    }

    public void DeleteDirectory(string path)
    {
        Mutate();
        Inner.DeleteDirectory(path);
    }

    public void SyncDirectory(string path)
    {
        CheckTargeted(path, FaultOps.SyncDirectory);
        Mutate();
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
            string? fragment = fs.FailNextAppendTo;
            bool targeted = fragment is not null && path.Contains(fragment, StringComparison.Ordinal);
            if (targeted) fs.FailNextAppendTo = null;
            if (fs.Tick() || targeted)
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
            if (fs.Tick()) throw fs.InjectedError();
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
            if (fs.Tick()) throw fs.InjectedError();
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
