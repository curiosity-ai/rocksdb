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
}
