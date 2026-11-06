using System.Buffers;
using System.Runtime.CompilerServices;
using Microsoft.Win32.SafeHandles;

namespace RocksDb.Managed.Env;

/// <summary>
/// <see cref="IFileSystem"/> over the local disk using <see cref="RandomAccess"/> on <see cref="SafeFileHandle"/>.
/// Port of env/fs_posix.cc (PosixFileSystem) and env/io_posix.cc (PosixWritableFile, PosixRandomAccessFile,
/// PosixSequentialFile, PosixDirectory), with the buffering of file/writable_file_writer.cc.
/// <list type="bullet">
/// <item>Errors surface as <see cref="RocksDbException"/> with <see cref="StatusCode.IOError"/> (sub code
/// <see cref="StatusSubCode.PathNotFound"/> for missing files/directories), like IOError() in io_posix.cc.</item>
/// <item><see cref="LockFile"/>: exclusive whole-file lock compatible with native RocksDB's
/// fcntl(F_SETLK) lock on Linux (via OFD locks), plus a process-wide set of locked paths like PosixFileSystem's
/// <c>lockedFiles</c>, so two opens of the same DB in this process fail as well.</item>
/// <item><see cref="SyncDirectory"/>: open + fsync of the directory on Unix; no-op on Windows.</item>
/// </list>
/// </summary>
public sealed class LocalFileSystem : IFileSystem
{
    public static readonly LocalFileSystem Instance = new();

    /// <summary>
    /// Whether <see cref="IWritableFile.SyncAsync"/> runs fdatasync on a dedicated sync thread instead of blocking the
    /// calling thread (typically a thread-pool thread) for the device flush. Off by default: durability and ordering are
    /// the same either way, but the hand-off adds two thread wake-ups per sync, which measured 15-53% higher latency for
    /// async synced writes (1-64 writers, see docs/perf.md). Turn it on when blocking pool threads on slow devices
    /// matters more than sync latency. Synchronous <see cref="IWritableFile.Sync"/> always runs on the caller.
    /// </summary>
    public bool OffloadAsyncSync { get; init; }

    // Process-wide set of locked files (PosixFileSystem's static lockedFiles). Guarded by s_lockedFilesLock.
    private static readonly HashSet<string> s_lockedFiles = new(StringComparer.Ordinal);
    private static readonly Lock s_lockedFilesLock = new();

    public IWritableFile CreateWritable(string path, bool truncate = true) =>
        new LocalWritableFile(path, truncate ? FileMode.Create : FileMode.OpenOrCreate, WritableFileOptions.Default, append: false, OffloadAsyncSync);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) =>
        new LocalWritableFile(path, truncate ? FileMode.Create : FileMode.OpenOrCreate, options, append: false, OffloadAsyncSync);

    public IWritableFile ReopenWritable(string path, WritableFileOptions options) =>
        new LocalWritableFile(path, FileMode.OpenOrCreate, options, append: true, OffloadAsyncSync);

    public IRandomAccessFile OpenRandomAccess(string path) => new LocalRandomAccessFile(path, RandomAccessFileOptions.Default);

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => new LocalRandomAccessFile(path, options);

    public ISequentialFile OpenSequential(string path) => new LocalSequentialFile(path, SequentialFileOptions.Default);

    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => new LocalSequentialFile(path, options);

    public bool FileExists(string path) => File.Exists(path);

    public bool DirectoryExists(string path) => Directory.Exists(path);

    public long GetFileSize(string path)
    {
        try
        {
            return new FileInfo(path).Length;
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("while stat a file for size", path, e);
        }
    }

    /// <summary>Deletes a file. Like File.Delete, deleting a missing file is not an error.</summary>
    public void DeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("while unlink() file", path, e);
        }
    }

    /// <summary>Atomically renames <paramref name="source"/> over <paramref name="target"/> (rename(2)).</summary>
    public void RenameFile(string source, string target)
    {
        try
        {
            // rename(2) also renames directories (used by Checkpoint to publish its temporary directory).
            if (Directory.Exists(source)) Directory.Move(source, target);
            else File.Move(source, target, overwrite: true);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While renaming a file to " + target, source, e);
        }
    }

    public void CreateDirectoryIfMissing(string path)
    {
        try
        {
            Directory.CreateDirectory(path);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While mkdir if missing", path, e);
        }
    }

    public void DeleteDirectory(string path)
    {
        try
        {
            Directory.Delete(path, recursive: false);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While rmdir", path, e);
        }
    }

    // EnumerationOptions.Compatible (what Directory.EnumerateFileSystemEntries uses): no attribute is skipped and
    // inaccessible entries are not ignored.
    private static readonly EnumerationOptions s_childrenEnumeration = new()
    {
        AttributesToSkip = 0,
        IgnoreInaccessible = false,
        MatchType = MatchType.Win32,
        RecurseSubdirectories = false,
    };

    /// <summary>Names (not paths) of the entries in <paramref name="directory"/>.</summary>
    public IEnumerable<string> GetChildren(string directory)
    {
        List<string> result = [];
        try
        {
            // Directory.EnumerateFileSystemEntries(directory) without building each entry's full path first: the
            // transform yields the name only (same entries: compatible options, nothing skipped, "." / ".." excluded).
            var entries = new System.IO.Enumeration.FileSystemEnumerable<string>(directory,
                static (ref System.IO.Enumeration.FileSystemEntry e) => e.FileName.ToString(), s_childrenEnumeration);
            foreach (string name in entries) result.Add(name);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While opendir", directory, e);
        }
        return result;
    }

    /// <summary>
    /// Acquires the DB "LOCK" (PosixFileSystem::LockFile). Fails with IOError if the lock is held by this process
    /// (any <see cref="LocalFileSystem"/>), by another process using this port, or by native RocksDB (Linux: OFD
    /// locks conflict with RocksDB's fcntl locks even within one process; Windows: exclusive share mode).
    /// On macOS only flock(2) is used, which does not exclude a native RocksDB process.
    /// </summary>
    public IDisposable LockFile(string path)
    {
        string key = Path.GetFullPath(path);
        lock (s_lockedFilesLock)
        {
            if (!s_lockedFiles.Add(key))
                throw RocksDbException.IOError("lock hold by current process: " + path + ": No locks available");
        }

        SafeFileHandle? handle = null;
        FileStream? stream = null;
        bool ofd = false;
        try
        {
            if (OperatingSystem.IsLinux())
            {
                handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
                int rc = UnixNative.TryOfdLock(handle);
                if (rc > 0) throw UnixNative.IOErrorFromErrno("While lock file", path, rc);
                ofd = rc == 0;
                if (rc < 0)
                {
                    // No OFD locks (old kernel): FileStream.Lock uses fcntl(F_SETLK), which still excludes other processes.
                    stream = new FileStream(handle, FileAccess.ReadWrite);
                    stream.Lock(0, 0);
                }
            }
            else
            {
                // Windows: exclusive share mode (like WinFileSystem::LockFile). macOS/BSD: .NET takes flock(LOCK_EX).
                handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            return new FileLock(key, handle, stream, ofd);
        }
        catch (Exception e)
        {
            stream?.Dispose();
            handle?.Dispose();
            lock (s_lockedFilesLock) s_lockedFiles.Remove(key);
            if (e is RocksDbException) throw;
            if (e is IOException or UnauthorizedAccessException) throw MapException("While lock file", path, e);
            throw;
        }
    }

    /// <summary>fsync the directory (PosixDirectory::Fsync). No-op on Windows.</summary>
    public void SyncDirectory(string path) => UnixNative.FsyncDirectory(path);

    /// <summary>Hard link (link(2) / CreateHardLinkW). Cross-device links throw NotSupported.</summary>
    public void LinkFile(string source, string target) => UnixNative.Link(source, target);

    /// <summary>statvfs f_bavail * f_frsize (PosixFileSystem::GetFreeSpace), through <see cref="DriveInfo"/>.</summary>
    public long GetFreeSpace(string path)
    {
        try
        {
            return new DriveInfo(Path.GetFullPath(path)).AvailableFreeSpace;
        }
        catch (Exception e) when (e is IOException or ArgumentException or UnauthorizedAccessException)
        {
            throw MapException("While doing statvfs", path, e);
        }
    }

    /// <summary>PosixRandomRWFile::Write + Sync on an existing file.</summary>
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data)
    {
        using SafeFileHandle h = OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, FileOptions.None);
        try
        {
            RandomAccess.Write(h, data, offset);
            RandomAccess.FlushToDisk(h);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While write random read/write file", path, e);
        }
    }

    public DateTimeOffset GetFileModificationTime(string path)
    {
        if (!File.Exists(path)) throw MapException("while stat a file for modification time", path, new FileNotFoundException(path));
        return new DateTimeOffset(File.GetLastWriteTimeUtc(path), TimeSpan.Zero);
    }

    internal static RocksDbException MapException(string context, string path, Exception e)
    {
        StatusSubCode sub = e is FileNotFoundException or DirectoryNotFoundException ? StatusSubCode.PathNotFound : StatusSubCode.None;
        return new RocksDbException(StatusCode.IOError, "IO error: " + context + ": " + path + ": " + e.Message, sub, e);
    }

    internal static SafeFileHandle OpenHandle(string path, FileMode mode, FileAccess access, FileShare share, FileOptions options)
    {
        try
        {
            return File.OpenHandle(path, mode, access, share, options);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw MapException("While open a file", path, e);
        }
    }

    private sealed class FileLock(string key, SafeFileHandle handle, FileStream? stream, bool ofd) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            // PosixFileSystem::UnlockFile: unlock, then close. The explicit unlock matters for OFD locks, which stay
            // held while a forked-but-not-yet-exec'd child (Process.Start on any thread) shares the description.
            // fcntl record locks are per process and .NET drops its flock before closing, so those need nothing more.
            if (ofd) UnixNative.OfdUnlock(handle);
            stream?.Dispose();
            handle.Dispose();
            lock (s_lockedFilesLock) s_lockedFiles.Remove(key);
        }
    }
}

/// <summary>
/// Port of PosixRandomAccessFile (env/io_posix.cc) + the read loop of RandomAccessFileReader. Positional reads
/// (pread) are thread-safe and may run concurrently. With direct I/O, reads are widened to aligned boundaries
/// through a pooled aligned bounce buffer.
/// </summary>
internal sealed class LocalRandomAccessFile : IRandomAccessFile
{
    private readonly SafeFileHandle _handle;
    private readonly string _path;
    private readonly int _alignment; // 0 when not using direct I/O

    public LocalRandomAccessFile(string path, RandomAccessFileOptions options)
    {
        _path = path;
        _handle = LocalFileSystem.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.RandomAccess | FileOptions.Asynchronous);
        try
        {
            if (options.UseDirectIO)
            {
                UnixNative.EnableDirectIO(_handle, path);
                _alignment = options.DirectIOAlignment;
            }
            Length = RandomAccess.GetLength(_handle);
        }
        catch
        {
            _handle.Dispose();
            throw;
        }
    }

    public long Length { get; }

    public int Read(long offset, Span<byte> destination)
    {
        if (_alignment != 0) return ReadDirect(offset, destination);
        try
        {
            int total = 0;
            while (total < destination.Length)
            {
                int n = RandomAccess.Read(_handle, destination[total..], offset + total);
                if (n == 0) break;
                total += n;
            }
            IOStatsContext.Add(IOStatsMetric.BytesRead, (ulong)total); // RandomAccessFileReader::Read: IOSTATS_ADD(bytes_read)
            return total;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While pread offset " + offset + " len " + destination.Length, _path, e);
        }
    }

    /// <summary>PosixRandomAccessFile::Prefetch: readahead(2) on Linux; not supported with direct I/O.</summary>
    public bool Prefetch(long offset, long length) => _alignment == 0 && UnixNative.TryReadahead(_handle, offset, length);

    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (_alignment != 0) return ReadDirectAsync(offset, destination, cancellationToken);
        ValueTask<int> first;
        if (UnixNative.NoWaitReadSupported)
        {
            // Page cache hit: complete synchronously instead of a thread pool round trip (RandomAccess.ReadAsync on
            // Unix runs a blocking pread on the pool). A short count means only part of the range is cached.
            int got = UnixNative.TryReadNoWait(_handle, destination.Span, offset);
            IOStatsContext.Add(IOStatsMetric.BytesRead, (ulong)Math.Max(got, 0));
            if (got == destination.Length || got == 0) return new ValueTask<int>(got);
            if (got > 0) return ReadAsyncSlow(default, got, offset, destination, cancellationToken);
        }
        try
        {
            first = RandomAccess.ReadAsync(_handle, destination, offset, cancellationToken);
        }
        catch (IOException e)
        {
            return ValueTask.FromException<int>(LocalFileSystem.MapException("While pread", _path, e));
        }
        if (first.IsCompletedSuccessfully)
        {
            // A ValueTask may be consumed only once: continue from the known count instead of re-awaiting it.
            int n = first.Result;
            if (n == destination.Length || n == 0) return new ValueTask<int>(n);
            return ReadAsyncSlow(default, n, offset, destination, cancellationToken);
        }
        return ReadAsyncSlow(first, -1, offset, destination, cancellationToken);
    }

    /// <summary>Completes a short read. Exactly one of <paramref name="first"/> (pending) or
    /// <paramref name="alreadyRead"/> (&gt;= 0) describes the first read.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadAsyncSlow(ValueTask<int> first, int alreadyRead, long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        try
        {
            int total = alreadyRead >= 0 ? alreadyRead : await first.ConfigureAwait(false);
            if (total == 0) return 0;
            while (total < destination.Length)
            {
                int n = await RandomAccess.ReadAsync(_handle, destination[total..], offset + total, cancellationToken).ConfigureAwait(false);
                if (n == 0) break;
                total += n;
            }
            return total;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While pread offset " + offset + " len " + destination.Length, _path, e);
        }
    }

    private int ReadDirect(long offset, Span<byte> destination)
    {
        if (destination.Length == 0) return 0;
        long alignedOffset = AlignedBuffer.RoundDown(offset, _alignment);
        int skip = (int)(offset - alignedOffset);
        int alignedLen = AlignedBuffer.RoundUp(skip + destination.Length, _alignment);
        AlignedBuffer buffer = AlignedBufferPool.Rent(alignedLen, _alignment);
        try
        {
            Span<byte> span = buffer.Span[..alignedLen];
            int total = 0;
            while (total < alignedLen)
            {
                int n = RandomAccess.Read(_handle, span[total..], alignedOffset + total);
                if (n == 0) break;
                total += n;
                if ((n & (_alignment - 1)) != 0) break; // short read: EOF
            }
            int available = Math.Max(0, Math.Min(total - skip, destination.Length));
            span.Slice(skip, available).CopyTo(destination);
            return available;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While pread (direct) offset " + offset, _path, e);
        }
        finally
        {
            AlignedBufferPool.Return(buffer);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadDirectAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (destination.Length == 0) return 0;
        long alignedOffset = AlignedBuffer.RoundDown(offset, _alignment);
        int skip = (int)(offset - alignedOffset);
        int alignedLen = AlignedBuffer.RoundUp(skip + destination.Length, _alignment);
        AlignedBuffer buffer = AlignedBufferPool.Rent(alignedLen, _alignment);
        try
        {
            Memory<byte> mem = buffer.Memory[..alignedLen];
            int total = 0;
            while (total < alignedLen)
            {
                int n = await RandomAccess.ReadAsync(_handle, mem[total..], alignedOffset + total, cancellationToken).ConfigureAwait(false);
                if (n == 0) break;
                total += n;
                if ((n & (_alignment - 1)) != 0) break;
            }
            int available = Math.Max(0, Math.Min(total - skip, destination.Length));
            mem.Slice(skip, available).CopyTo(destination);
            return available;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While pread (direct) offset " + offset, _path, e);
        }
        finally
        {
            AlignedBufferPool.Return(buffer);
        }
    }

    public void Dispose() => _handle.Dispose();
}

/// <summary>
/// Port of PosixSequentialFile + SequentialFileReader (file/sequence_file_reader.cc), including the optional
/// readahead wrapper (ReadaheadSequentialFile). Not thread-safe (single reader).
/// </summary>
internal sealed class LocalSequentialFile : ISequentialFile
{
    private readonly SafeFileHandle _handle;
    private readonly string _path;
    private long _position;          // file offset of the next byte returned to the caller
    private byte[]? _buffer;         // readahead buffer (null when readahead is off)
    private int _bufferStart;        // index in _buffer of the byte at _position
    private int _bufferEnd;          // number of valid bytes in _buffer

    public LocalSequentialFile(string path, SequentialFileOptions options)
    {
        _path = path;
        _handle = LocalFileSystem.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan | FileOptions.Asynchronous);
        if (options.ReadaheadSize > 0) _buffer = ArrayPool<byte>.Shared.Rent(options.ReadaheadSize);
    }

    private int Buffered => _bufferEnd - _bufferStart;

    public int Read(Span<byte> destination)
    {
        try
        {
            if (_buffer is null)
            {
                int n = RandomAccess.Read(_handle, destination, _position);
                _position += n;
                return n;
            }
            int total = 0;
            while (total < destination.Length)
            {
                if (Buffered == 0)
                {
                    int remaining = destination.Length - total;
                    if (remaining >= _buffer.Length)
                    {
                        int d = RandomAccess.Read(_handle, destination[total..], _position);
                        _position += d;
                        return total + d;
                    }
                    _bufferStart = 0;
                    _bufferEnd = RandomAccess.Read(_handle, _buffer, _position);
                    if (_bufferEnd == 0) break;
                }
                int take = Math.Min(Buffered, destination.Length - total);
                _buffer.AsSpan(_bufferStart, take).CopyTo(destination[total..]);
                _bufferStart += take;
                _position += take;
                total += take;
            }
            return total;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While reading file sequentially", _path, e);
        }
    }

    public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (_buffer is not null && Buffered >= destination.Length)
        {
            _buffer.AsSpan(_bufferStart, destination.Length).CopyTo(destination.Span);
            _bufferStart += destination.Length;
            _position += destination.Length;
            return new ValueTask<int>(destination.Length);
        }
        return ReadAsyncSlow(destination, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<int> ReadAsyncSlow(Memory<byte> destination, CancellationToken cancellationToken)
    {
        try
        {
            if (_buffer is null)
            {
                int n = await RandomAccess.ReadAsync(_handle, destination, _position, cancellationToken).ConfigureAwait(false);
                _position += n;
                return n;
            }
            int total = 0;
            while (total < destination.Length)
            {
                if (Buffered == 0)
                {
                    int remaining = destination.Length - total;
                    if (remaining >= _buffer.Length)
                    {
                        int d = await RandomAccess.ReadAsync(_handle, destination[total..], _position, cancellationToken).ConfigureAwait(false);
                        _position += d;
                        return total + d;
                    }
                    _bufferStart = 0;
                    _bufferEnd = await RandomAccess.ReadAsync(_handle, _buffer, _position, cancellationToken).ConfigureAwait(false);
                    if (_bufferEnd == 0) break;
                }
                int take = Math.Min(Buffered, destination.Length - total);
                _buffer.AsMemory(_bufferStart, take).CopyTo(destination[total..]);
                _bufferStart += take;
                _position += take;
                total += take;
            }
            return total;
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While reading file sequentially", _path, e);
        }
    }

    public void Skip(long bytes)
    {
        if (_buffer is not null)
        {
            if (bytes <= Buffered)
            {
                _bufferStart += (int)bytes;
            }
            else
            {
                _bufferStart = _bufferEnd = 0;
            }
        }
        _position += bytes;
    }

    public void Dispose()
    {
        _handle.Dispose();
        byte[]? b = _buffer;
        _buffer = null;
        if (b is not null) ArrayPool<byte>.Shared.Return(b);
    }
}

/// <summary>
/// Port of WritableFileWriter (file/writable_file_writer.cc) over PosixWritableFile (env/io_posix.cc).
/// <list type="bullet">
/// <item>Buffered mode: a pooled buffer that grows from <see cref="WritableFileOptions.InitialBufferSize"/> up to
/// <see cref="WritableFileOptions.MaxBufferSize"/>; appends larger than the buffer are written through.</item>
/// <item>Direct mode: aligned native buffer; whole pages are written and the partial tail page is kept and
/// rewritten on the next flush (WriteDirect); the file is truncated to its logical size on close.</item>
/// <item><see cref="Sync"/> = flush + fdatasync on Linux (fsync/F_FULLFSYNC elsewhere).</item>
/// <item>Optional fallocate preallocation, bytes_per_sync range syncs and a rate limiter hook.</item>
/// </list>
/// Not thread-safe: a writable file has a single writer, as in RocksDB.
/// Async methods complete synchronously while data fits in the buffer; slow paths use pooled state machines.
/// </summary>
internal sealed class LocalWritableFile : IWritableFile
{
    private readonly SafeFileHandle _handle;
    private readonly string _path;
    private readonly WritableFileOptions _options;
    private readonly int _alignment;           // 0 when not using direct I/O
    private byte[]? _array;                    // buffered mode
    private AlignedBuffer? _aligned;           // direct mode
    private int _capacity;
    private int _buffered;
    private long _filePos;                     // buffered: bytes handed to the OS; direct: aligned offset of buffer[0]
    private long _preallocatedEnd;
    private long _lastRangeSync;
    private bool _preallocationFailed;
    private bool _disposed;
    private readonly bool _offloadAsyncSync;  // LocalFileSystem.OffloadAsyncSync

    public LocalWritableFile(string path, FileMode mode, WritableFileOptions options, bool append, bool offloadAsyncSync)
    {
        _path = path;
        _offloadAsyncSync = offloadAsyncSync;
        _options = options;
        // FileMode.Create opens with O_CREAT and then always calls ftruncate(fd, 0), even on a file it just created. On
        // ext4 a truncate to size 0 arms auto_da_alloc's "replace via truncate" heuristic, which starts the writeback of
        // the file's delayed allocations when it is closed. Upstream (PosixFileSystem::NewWritableFile) opens with
        // O_CREAT | O_TRUNC, which truncates only a file that already existed; do the same.
        bool truncate = mode == FileMode.Create;
        _handle = LocalFileSystem.OpenHandle(path, truncate ? FileMode.OpenOrCreate : mode, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete,
            FileOptions.Asynchronous);
        try
        {
            if (truncate && RandomAccess.GetLength(_handle) > 0) RandomAccess.SetLength(_handle, 0);
            if (append) _filePos = RandomAccess.GetLength(_handle);
            if (options.UseDirectIO)
            {
                UnixNative.EnableDirectIO(_handle, path);
                _alignment = options.DirectIOAlignment;
                _capacity = AlignedBuffer.RoundUp(Math.Max(options.InitialBufferSize, _alignment), _alignment);
                _aligned = new AlignedBuffer(_capacity, _alignment);
                if (append && (_filePos & (_alignment - 1)) != 0)
                {
                    // Re-read the partial tail page so that it can be rewritten in full.
                    long alignedPos = AlignedBuffer.RoundDown(_filePos, _alignment);
                    int tail = (int)(_filePos - alignedPos);
                    int read = RandomAccess.Read(_handle, _aligned.Span[.._alignment], alignedPos);
                    if (read < tail) ThrowHelper.Corruption("short read of tail page while reopening " + path);
                    _filePos = alignedPos;
                    _buffered = tail;
                }
            }
            else
            {
                _capacity = Math.Max(1, Math.Min(options.InitialBufferSize, options.MaxBufferSize));
                _array = ArrayPool<byte>.Shared.Rent(_capacity);
                _capacity = _array.Length;
            }
            _lastRangeSync = _filePos;
            _preallocatedEnd = _filePos;
        }
        catch
        {
            _handle.Dispose();
            if (_aligned is not null) ((IDisposable)_aligned).Dispose();
            throw;
        }
    }

    public long Size => _filePos + _buffered;

    private Span<byte> BufferSpan => _array is not null ? _array.AsSpan(0, _capacity) : _aligned!.Span;
    private Memory<byte> BufferMemory => _array is not null ? _array.AsMemory(0, _capacity) : _aligned!.Memory;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> data)
    {
        if (data.Length <= _capacity - _buffered)
        {
            data.CopyTo(BufferSpan[_buffered..]);
            _buffered += data.Length;
            return;
        }
        AppendSlow(data);
    }

    private void AppendSlow(ReadOnlySpan<byte> data)
    {
        ThrowIfDisposed();
        if (_alignment == 0 && data.Length >= _capacity)
        {
            // An append at least as large as the buffer (e.g. the table builder's staged blocks) is written through
            // instead of growing the buffer and copying it once more.
            Flush();
            WriteRaw(data, _filePos);
            _filePos += data.Length;
            MaybeRangeSync();
            return;
        }
        if (TryGrowBuffer(data.Length))
        {
            data.CopyTo(BufferSpan[_buffered..]);
            _buffered += data.Length;
            return;
        }
        if (_alignment != 0)
        {
            while (!data.IsEmpty)
            {
                int n = Math.Min(data.Length, _capacity - _buffered);
                data[..n].CopyTo(BufferSpan[_buffered..]);
                _buffered += n;
                data = data[n..];
                if (_buffered == _capacity) FlushDirect();
            }
            return;
        }
        Flush();
        if (data.Length >= _capacity)
        {
            WriteRaw(data, _filePos);
            _filePos += data.Length;
        }
        else
        {
            data.CopyTo(BufferSpan);
            _buffered = data.Length;
        }
    }

    public ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (data.Length <= _capacity - _buffered)
        {
            data.Span.CopyTo(BufferSpan[_buffered..]);
            _buffered += data.Length;
            return ValueTask.CompletedTask;
        }
        if (_alignment == 0 && data.Length < _capacity && TryGrowBuffer(data.Length))
        {
            data.Span.CopyTo(BufferSpan[_buffered..]);
            _buffered += data.Length;
            return ValueTask.CompletedTask;
        }
        return AppendSlowAsync(data, cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AppendSlowAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (_alignment != 0)
        {
            while (!data.IsEmpty)
            {
                int n = Math.Min(data.Length, _capacity - _buffered);
                data[..n].CopyTo(BufferMemory[_buffered..]);
                _buffered += n;
                data = data[n..];
                if (_buffered == _capacity) await FlushDirectAsync(cancellationToken).ConfigureAwait(false);
            }
            return;
        }
        await FlushAsync(cancellationToken).ConfigureAwait(false);
        if (data.Length >= _capacity)
        {
            await WriteRawAsync(data, _filePos, cancellationToken).ConfigureAwait(false);
            _filePos += data.Length;
        }
        else
        {
            data.CopyTo(BufferMemory);
            _buffered = data.Length;
        }
    }

    /// <summary>Grows the pooled buffer (buffered mode only) so that <paramref name="needed"/> more bytes fit.</summary>
    private bool TryGrowBuffer(int needed)
    {
        if (_array is null || _capacity >= _options.MaxBufferSize) return false;
        long target = (long)_buffered + needed;
        if (target > _options.MaxBufferSize) return false;
        int newSize = _capacity;
        while (newSize < target) newSize = (int)Math.Min((long)newSize * 2, _options.MaxBufferSize);
        byte[] bigger = ArrayPool<byte>.Shared.Rent(newSize);
        _array.AsSpan(0, _buffered).CopyTo(bigger);
        ArrayPool<byte>.Shared.Return(_array);
        _array = bigger;
        _capacity = bigger.Length;
        return true;
    }

    public void Flush()
    {
        if (_buffered == 0) return;
        ThrowIfDisposed();
        if (_alignment != 0)
        {
            FlushDirect();
            return;
        }
        WriteRaw(_array.AsSpan(0, _buffered), _filePos);
        _filePos += _buffered;
        _buffered = 0;
        MaybeRangeSync();
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_buffered == 0) return ValueTask.CompletedTask;
        return FlushSlowAsync(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask FlushSlowAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (_alignment != 0)
        {
            await FlushDirectAsync(cancellationToken).ConfigureAwait(false);
            return;
        }
        await WriteRawAsync(_array.AsMemory(0, _buffered), _filePos, cancellationToken).ConfigureAwait(false);
        _filePos += _buffered;
        _buffered = 0;
        MaybeRangeSync();
    }

    // Direct mode: write every page that holds buffered data (zero padding the last one), then keep the
    // partial tail page at the start of the buffer (WritableFileWriter::WriteDirect).
    private int PrepareDirectWrite(out int fileAdvance, out int leftover)
    {
        fileAdvance = (int)AlignedBuffer.RoundDown(_buffered, _alignment);
        leftover = _buffered - fileAdvance;
        int writeLen = AlignedBuffer.RoundUp(_buffered, _alignment);
        BufferSpan[_buffered..writeLen].Clear();
        return writeLen;
    }

    private void FinishDirectWrite(int fileAdvance, int leftover)
    {
        if (leftover > 0) BufferSpan.Slice(fileAdvance, leftover).CopyTo(BufferSpan);
        _filePos += fileAdvance;
        _buffered = leftover;
        MaybeRangeSync();
    }

    private void FlushDirect()
    {
        if (_buffered == 0) return;
        int writeLen = PrepareDirectWrite(out int fileAdvance, out int leftover);
        WriteRaw(BufferSpan[..writeLen], _filePos);
        FinishDirectWrite(fileAdvance, leftover);
    }

    private async ValueTask FlushDirectAsync(CancellationToken cancellationToken)
    {
        if (_buffered == 0) return;
        int writeLen = PrepareDirectWrite(out int fileAdvance, out int leftover);
        await WriteRawAsync(BufferMemory[..writeLen], _filePos, cancellationToken).ConfigureAwait(false);
        FinishDirectWrite(fileAdvance, leftover);
    }

    private void WriteRaw(ReadOnlySpan<byte> data, long offset)
    {
        IOStatsContext.Add(IOStatsMetric.BytesWritten, (ulong)data.Length); // WritableFileWriter: IOSTATS_ADD(bytes_written)
        Preallocate(offset + data.Length);
        try
        {
            IRateLimiter? limiter = _options.RateLimiter;
            if (limiter is null)
            {
                RandomAccess.Write(_handle, data, offset);
                return;
            }
            while (!data.IsEmpty)
            {
                int chunk = (int)Math.Min(data.Length, Math.Max(1, limiter.SingleBurstBytes));
                if (_alignment != 0) chunk = Math.Max(_alignment, (int)AlignedBuffer.RoundDown(chunk, _alignment));
                chunk = Math.Min(chunk, data.Length);
                limiter.Request(chunk, _options.IoPriority, _options.Statistics, RateLimiterOpType.Write);
                RandomAccess.Write(_handle, data[..chunk], offset);
                offset += chunk;
                data = data[chunk..];
            }
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While appending to file", _path, e);
        }
    }

    private ValueTask WriteRawAsync(ReadOnlyMemory<byte> data, long offset, CancellationToken cancellationToken)
    {
        IOStatsContext.Add(IOStatsMetric.BytesWritten, (ulong)data.Length);
        if (_alignment == 0 && _options.RateLimiter is null && !data.IsEmpty)
        {
            if (UnixNative.NoWaitWriteSupported)
            {
                // Buffered write that fits into the page cache without blocking: complete synchronously instead of a
                // thread pool round trip per WAL record / buffer flush. Falls back for the rest when it would block.
                Preallocate(offset + data.Length);
                int n = UnixNative.TryWriteNoWait(_handle, data.Span, offset);
                if (n == data.Length) return ValueTask.CompletedTask;
                if (n > 0)
                {
                    data = data[n..];
                    offset += n;
                }
            }
            else if (data.Length <= InlineAsyncWriteLimit)
            {
                // No RWF_NOWAIT for buffered writes (e.g. ext4): a small buffered pwrite is a copy into the page cache,
                // much cheaper than the thread pool round trip RandomAccess.WriteAsync makes on Unix (it runs the same
                // blocking pwrite on a pool thread). Native RocksDB writes the WAL the same way (the
                // fdatasync of SyncAsync, which blocks much longer, runs on a dedicated sync thread).
                try
                {
                    WriteRaw(data.Span, offset);
                    return ValueTask.CompletedTask;
                }
                catch (Exception e)
                {
                    return ValueTask.FromException(e);
                }
            }
        }
        return WriteRawSlowAsync(data, offset, cancellationToken);
    }

    /// <summary>Largest buffered write <see cref="WriteRawAsync"/> performs inline when RWF_NOWAIT is unavailable.</summary>
    private const int InlineAsyncWriteLimit = 256 << 10;

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WriteRawSlowAsync(ReadOnlyMemory<byte> data, long offset, CancellationToken cancellationToken)
    {
        Preallocate(offset + data.Length);
        try
        {
            IRateLimiter? limiter = _options.RateLimiter;
            if (limiter is null)
            {
                await RandomAccess.WriteAsync(_handle, data, offset, cancellationToken).ConfigureAwait(false);
                return;
            }
            while (!data.IsEmpty)
            {
                int chunk = (int)Math.Min(data.Length, Math.Max(1, limiter.SingleBurstBytes));
                if (_alignment != 0) chunk = Math.Max(_alignment, (int)AlignedBuffer.RoundDown(chunk, _alignment));
                chunk = Math.Min(chunk, data.Length);
                await limiter.RequestAsync(chunk, _options.IoPriority, _options.Statistics, RateLimiterOpType.Write, cancellationToken).ConfigureAwait(false);
                await RandomAccess.WriteAsync(_handle, data[..chunk], offset, cancellationToken).ConfigureAwait(false);
                offset += chunk;
                data = data[chunk..];
            }
        }
        catch (IOException e)
        {
            throw LocalFileSystem.MapException("While appending to file", _path, e);
        }
    }

    // PosixWritableFile::PrepareWrite: reserve whole preallocation blocks covering the write.
    private void Preallocate(long end)
    {
        long block = _options.PreallocationBlockSize;
        if (block <= 0 || _preallocationFailed || end <= _preallocatedEnd) return;
        long newEnd = (end + block - 1) / block * block;
        if (UnixNative.TryAllocate(_handle, _preallocatedEnd, newEnd - _preallocatedEnd))
            _preallocatedEnd = newEnd;
        else
            _preallocationFailed = true;
    }

    // WritableFileWriter: bytes_per_sync => RangeSync(last_sync_size_, offset_sync_to - last_sync_size_).
    private void MaybeRangeSync()
    {
        long bps = _options.BytesPerSync;
        if (bps <= 0) return;
        long syncTo = AlignedBuffer.RoundDown(_filePos, 4096);
        if (syncTo - _lastRangeSync >= bps)
        {
            UnixNative.TryRangeSync(_handle, _lastRangeSync, syncTo - _lastRangeSync);
            _lastRangeSync = syncTo;
        }
    }

    public void Sync()
    {
        Flush();
        long perfSync = PerfContext.StartTimer(); // IOSTATS_TIMER_GUARD(fsync_nanos)
        UnixNative.DataSync(_handle, _path);
        IOStatsContext.StopTimer(IOStatsMetric.FsyncNanos, perfSync);
    }

    public ValueTask SyncAsync(CancellationToken cancellationToken = default)
    {
        if (_buffered == 0)
        {
            try
            {
                ThrowIfDisposed();
                if (!_offloadAsyncSync)
                {
                    UnixNative.DataSync(_handle, _path);
                    return ValueTask.CompletedTask;
                }
            }
            catch (Exception e)
            {
                return ValueTask.FromException(e);
            }
            // The fdatasync runs on a dedicated sync thread (FileSyncWorkers): the caller's thread is not blocked for
            // the device flush. The data was written to the OS before, so the queued sync covers it.
            return FileSyncWorkers.DataSyncAsync(_handle, _path);
        }
        return SyncSlowAsync(cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask SyncSlowAsync(CancellationToken cancellationToken)
    {
        await FlushAsync(cancellationToken).ConfigureAwait(false);
        if (_offloadAsyncSync) await FileSyncWorkers.DataSyncAsync(_handle, _path).ConfigureAwait(false);
        else UnixNative.DataSync(_handle, _path);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) ThrowHelper.InvalidArgument("file already closed: " + _path);
    }

    // PosixWritableFile::Close: trim to the logical size when the tail was padded (direct I/O) or blocks were
    // preallocated past EOF.
    private void TruncateOnClose()
    {
        if (_alignment != 0 || _preallocatedEnd > Size || _options.TruncateToSizeOnClose)
        {
            try
            {
                RandomAccess.SetLength(_handle, Size);
            }
            catch (IOException e)
            {
                throw LocalFileSystem.MapException("While ftruncate", _path, e);
            }
        }
    }

    private void ReleaseResources()
    {
        _disposed = true;
        _handle.Dispose();
        if (_array is not null)
        {
            ArrayPool<byte>.Shared.Return(_array);
            _array = null;
        }
        if (_aligned is not null)
        {
            ((IDisposable)_aligned).Dispose();
            _aligned = null;
        }
        _capacity = 0;
    }

    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            Flush();
            TruncateOnClose();
        }
        finally
        {
            ReleaseResources();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            await FlushAsync().ConfigureAwait(false);
            TruncateOnClose();
        }
        finally
        {
            ReleaseResources();
        }
    }
}
