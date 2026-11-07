namespace RocksDb.Managed.Replication.Tests.Infrastructure;

/// <summary>
/// Forwards everything to an inner file system; while <see cref="Arm"/>ed, the next fsync of a WAL file (*.log) blocks
/// until <see cref="Release"/> - a write that is in the middle of being applied (not yet visible: the sequence number
/// is published after the WAL sync), for tests that need another event to land exactly then.
/// </summary>
public sealed class GatedFileSystem(IFileSystem inner) : IFileSystem
{
    private volatile bool _armed;
    private TaskCompletionSource _blocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Arm()
    {
        _blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _armed = true;
    }

    /// <summary>Completes when a WAL sync is blocked.</summary>
    public Task Blocked => _blocked.Task;

    public void Release()
    {
        _armed = false;
        _release.TrySetResult();
    }

    private bool TryBlock(string path, out Task release)
    {
        release = Task.CompletedTask;
        if (!_armed || !path.EndsWith(".log", StringComparison.Ordinal)) return false;
        _armed = false;
        release = _release.Task;
        _blocked.TrySetResult();
        return true;
    }

    private IWritableFile Wrap(string path, IWritableFile f) => new Writable(this, path, f);

    public IWritableFile CreateWritable(string path, bool truncate = true) => Wrap(path, inner.CreateWritable(path, truncate));
    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => Wrap(path, inner.CreateWritable(path, truncate, options));
    public IWritableFile ReopenWritable(string path, WritableFileOptions options) => Wrap(path, inner.ReopenWritable(path, options));
    public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) => Wrap(path, inner.ReuseWritable(path, oldPath, options));
    public IRandomAccessFile OpenRandomAccess(string path) => inner.OpenRandomAccess(path);
    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => inner.OpenRandomAccess(path, options);
    public ISequentialFile OpenSequential(string path) => inner.OpenSequential(path);
    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => inner.OpenSequential(path, options);
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
    public DateTimeOffset GetFileModificationTime(string path) => inner.GetFileModificationTime(path);
    public long GetFreeSpace(string path) => inner.GetFreeSpace(path);
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data) => inner.WriteAt(path, offset, data);

    private sealed class Writable(GatedFileSystem fs, string path, IWritableFile inner) : IWritableFile
    {
        public long Size => inner.Size;
        public void Append(ReadOnlySpan<byte> data) => inner.Append(data);
        public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) => inner.AppendAsync(data, cancellationToken);
        public ValueTask FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);
        public void Flush() => inner.Flush();

        public void Sync()
        {
            if (fs.TryBlock(path, out Task release)) release.Wait();
            inner.Sync();
        }

        public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
        {
            if (fs.TryBlock(path, out Task release)) await release.ConfigureAwait(false);
            await inner.SyncAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Dispose() => inner.Dispose();
        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
