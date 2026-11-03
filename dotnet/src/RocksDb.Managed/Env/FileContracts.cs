using Microsoft.Win32.SafeHandles;

namespace RocksDb.Managed.Env;

/// <summary>
/// CONTRACT (owned by stream F). Append-only file used for WAL, MANIFEST, SST output. Port of the
/// WritableFile/WritableFileWriter pair (file/writable_file_writer.h). Implementations buffer internally;
/// <see cref="Append"/> copies the data so callers may reuse their buffers immediately.
/// </summary>
public interface IWritableFile : IAsyncDisposable, IDisposable
{
    /// <summary>Logical size (bytes appended so far, including buffered).</summary>
    long Size { get; }
    void Append(ReadOnlySpan<byte> data);
    ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default);
    /// <summary>Pushes buffered data to the OS.</summary>
    ValueTask FlushAsync(CancellationToken cancellationToken = default);
    /// <summary>Flush + fsync/fdatasync.</summary>
    ValueTask SyncAsync(CancellationToken cancellationToken = default);
    void Flush();
    void Sync();
}

/// <summary>CONTRACT (owned by stream F). Positional reads (SST, blob files). Port of RandomAccessFileReader. Thread-safe.</summary>
public interface IRandomAccessFile : IDisposable
{
    long Length { get; }
    /// <summary>Reads up to destination.Length bytes at offset; returns bytes read (short only at EOF).</summary>
    int Read(long offset, Span<byte> destination);
    ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default);
}

/// <summary>CONTRACT (owned by stream F). Sequential reads (WAL/MANIFEST replay). Port of SequentialFileReader.</summary>
public interface ISequentialFile : IDisposable
{
    int Read(Span<byte> destination);
    ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default);
    void Skip(long bytes);
}

/// <summary>CONTRACT (owned by stream F). File system abstraction (port of rocksdb::FileSystem / Env subset).</summary>
public interface IFileSystem
{
    IWritableFile CreateWritable(string path, bool truncate = true);
    IRandomAccessFile OpenRandomAccess(string path);
    ISequentialFile OpenSequential(string path);
    bool FileExists(string path);
    long GetFileSize(string path);
    void DeleteFile(string path);
    void RenameFile(string source, string target);
    void CreateDirectoryIfMissing(string path);
    IEnumerable<string> GetChildren(string directory);
    /// <summary>Acquires an exclusive advisory lock (the DB "LOCK" file). Dispose to release.</summary>
    IDisposable LockFile(string path);
    /// <summary>fsync a directory so that renames/creates are durable (no-op where unsupported).</summary>
    void SyncDirectory(string path);

    // ---- Additions (stream F). Default implementations keep third-party implementations source compatible. ----

    /// <summary>Creates a writable file with explicit buffering/sync/direct-I/O options.</summary>
    IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) => CreateWritable(path, truncate);

    /// <summary>
    /// Opens an existing file (creating it if missing) for appending at its current end
    /// (FileSystem::ReopenWritableFile).
    /// </summary>
    IWritableFile ReopenWritable(string path, WritableFileOptions options) =>
        throw RocksDbException.NotSupported("ReopenWritable is not supported by " + GetType().Name);

    /// <summary>Opens a random-access file with explicit options (e.g. direct I/O).</summary>
    IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => OpenRandomAccess(path);

    /// <summary>Opens a sequential file with explicit options (e.g. readahead).</summary>
    ISequentialFile OpenSequential(string path, SequentialFileOptions options) => OpenSequential(path);

    /// <summary>True if <paramref name="path"/> names an existing directory.</summary>
    bool DirectoryExists(string path) =>
        throw RocksDbException.NotSupported("DirectoryExists is not supported by " + GetType().Name);

    /// <summary>Deletes an empty directory (FileSystem::DeleteDir).</summary>
    void DeleteDirectory(string path) =>
        throw RocksDbException.NotSupported("DeleteDirectory is not supported by " + GetType().Name);

    /// <summary>
    /// Creates a hard link <paramref name="target"/> to the existing file <paramref name="source"/>
    /// (FileSystem::LinkFile). Fails if <paramref name="target"/> exists. Throws <see cref="StatusCode.NotSupported"/>
    /// when the file system (or the pair of directories, e.g. across devices) does not support hard links; callers
    /// such as Checkpoint then fall back to copying.
    /// </summary>
    void LinkFile(string source, string target) =>
        throw RocksDbException.NotSupported("LinkFile is not supported by " + GetType().Name);

    /// <summary>
    /// Overwrites <paramref name="data"/> at <paramref name="offset"/> of an existing file and syncs it (an
    /// FSRandomRWFile Write + Sync, see FileSystem::NewRandomRWFile). Throws <see cref="StatusCode.NotSupported"/> when
    /// the file system has no random writes.
    /// </summary>
    void WriteAt(string path, long offset, ReadOnlySpan<byte> data) =>
        throw RocksDbException.NotSupported("WriteAt is not supported by " + GetType().Name);

    /// <summary>Last modification time of a file (FileSystem::GetFileModificationTime), used by WAL archive TTL purging.</summary>
    DateTimeOffset GetFileModificationTime(string path) =>
        throw RocksDbException.NotSupported("GetFileModificationTime is not supported by " + GetType().Name);

    /// <summary>
    /// Bytes available to the process on the file system holding <paramref name="path"/> (FileSystem::GetFreeSpace), used by
    /// the <see cref="SstFileManager"/> to recover from out-of-space errors. Throws NotSupported when unknown.
    /// </summary>
    long GetFreeSpace(string path) =>
        throw RocksDbException.NotSupported("GetFreeSpace is not supported by " + GetType().Name);
}
