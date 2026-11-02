using Microsoft.Win32.SafeHandles;

namespace RocksDb.Managed.Env;

/// <summary>
/// Minimal working <see cref="IFileSystem"/> over <see cref="RandomAccess"/>/<see cref="SafeFileHandle"/>, so that
/// other streams can do real file I/O immediately. Stream F replaces/extends it (write buffering policy, direct I/O,
/// rate limiting, proper fdatasync, directory fsync, readahead).
/// </summary>
public sealed class LocalFileSystem : IFileSystem
{
    public static readonly LocalFileSystem Instance = new();

    public IWritableFile CreateWritable(string path, bool truncate = true) => new LocalWritableFile(path, truncate);
    public IRandomAccessFile OpenRandomAccess(string path) => new LocalRandomAccessFile(path);
    public ISequentialFile OpenSequential(string path) => new LocalSequentialFile(path);
    public bool FileExists(string path) => File.Exists(path);
    public long GetFileSize(string path) => new FileInfo(path).Length;
    public void DeleteFile(string path) => File.Delete(path);
    public void RenameFile(string source, string target) => File.Move(source, target, overwrite: true);
    public void CreateDirectoryIfMissing(string path) => Directory.CreateDirectory(path);
    public IEnumerable<string> GetChildren(string directory) => Directory.EnumerateFileSystemEntries(directory).Select(Path.GetFileName)!;
    public IDisposable LockFile(string path) => new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    public void SyncDirectory(string path) { /* TODO(stream F): fsync directory on Unix */ }
}

internal sealed class LocalRandomAccessFile : IRandomAccessFile
{
    private readonly SafeFileHandle _handle;
    public LocalRandomAccessFile(string path)
    {
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.RandomAccess | FileOptions.Asynchronous);
        Length = RandomAccess.GetLength(_handle);
    }
    public long Length { get; }
    public int Read(long offset, Span<byte> destination)
    {
        int total = 0;
        while (total < destination.Length)
        {
            int n = RandomAccess.Read(_handle, destination[total..], offset + total);
            if (n == 0) break;
            total += n;
        }
        return total;
    }
    public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        int total = 0;
        while (total < destination.Length)
        {
            int n = await RandomAccess.ReadAsync(_handle, destination[total..], offset + total, cancellationToken).ConfigureAwait(false);
            if (n == 0) break;
            total += n;
        }
        return total;
    }
    public void Dispose() => _handle.Dispose();
}

internal sealed class LocalSequentialFile : ISequentialFile
{
    private readonly SafeFileHandle _handle;
    private long _position;
    public LocalSequentialFile(string path) =>
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan | FileOptions.Asynchronous);
    public int Read(Span<byte> destination)
    {
        int n = RandomAccess.Read(_handle, destination, _position);
        _position += n;
        return n;
    }
    public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        int n = await RandomAccess.ReadAsync(_handle, destination, _position, cancellationToken).ConfigureAwait(false);
        _position += n;
        return n;
    }
    public void Skip(long bytes) => _position += bytes;
    public void Dispose() => _handle.Dispose();
}

internal sealed class LocalWritableFile : IWritableFile
{
    private const int BufferSize = 64 * 1024;
    private readonly SafeFileHandle _handle;
    private readonly byte[] _buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(BufferSize);
    private int _buffered;
    private long _flushedOffset;

    public LocalWritableFile(string path, bool truncate) =>
        _handle = File.OpenHandle(path, truncate ? FileMode.Create : FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read | FileShare.Delete, FileOptions.Asynchronous);

    public long Size => _flushedOffset + _buffered;

    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.Length > _buffer.Length - _buffered)
        {
            Flush();
            if (data.Length >= _buffer.Length)
            {
                RandomAccess.Write(_handle, data, _flushedOffset);
                _flushedOffset += data.Length;
                return;
            }
        }
        data.CopyTo(_buffer.AsSpan(_buffered));
        _buffered += data.Length;
    }

    public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (data.Length <= _buffer.Length - _buffered)
        {
            data.Span.CopyTo(_buffer.AsSpan(_buffered));
            _buffered += data.Length;
            return ValueTask.CompletedTask;
        }
        return AppendSlowAsync(data, cancellationToken);
    }

    private async ValueTask AppendSlowAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        await FlushAsync(cancellationToken).ConfigureAwait(false);
        if (data.Length >= _buffer.Length)
        {
            await RandomAccess.WriteAsync(_handle, data, _flushedOffset, cancellationToken).ConfigureAwait(false);
            _flushedOffset += data.Length;
        }
        else
        {
            data.Span.CopyTo(_buffer);
            _buffered = data.Length;
        }
    }

    public void Flush()
    {
        if (_buffered == 0) return;
        RandomAccess.Write(_handle, _buffer.AsSpan(0, _buffered), _flushedOffset);
        _flushedOffset += _buffered;
        _buffered = 0;
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_buffered == 0) return;
        await RandomAccess.WriteAsync(_handle, _buffer.AsMemory(0, _buffered), _flushedOffset, cancellationToken).ConfigureAwait(false);
        _flushedOffset += _buffered;
        _buffered = 0;
    }

    public void Sync()
    {
        Flush();
        RandomAccess.FlushToDisk(_handle);
    }

    public async ValueTask SyncAsync(CancellationToken cancellationToken = default)
    {
        await FlushAsync(cancellationToken).ConfigureAwait(false);
        RandomAccess.FlushToDisk(_handle);
    }

    public void Dispose()
    {
        Flush();
        _handle.Dispose();
        System.Buffers.ArrayPool<byte>.Shared.Return(_buffer);
    }

    public async ValueTask DisposeAsync()
    {
        await FlushAsync().ConfigureAwait(false);
        _handle.Dispose();
        System.Buffers.ArrayPool<byte>.Shared.Return(_buffer);
    }
}
