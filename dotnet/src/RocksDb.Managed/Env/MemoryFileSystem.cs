namespace RocksDb.Managed.Env;

/// <summary>
/// In-memory <see cref="IFileSystem"/> for fast unit tests. Port of the idea of env/mock_env.cc (MockFileSystem)
/// combined with a lite FaultInjectionTestFS (utilities/fault_injection_fs.{h,cc}):
/// <list type="bullet">
/// <item>Each file tracks how many bytes were made durable by <see cref="IWritableFile.Sync"/>;
/// <see cref="DropUnsyncedFileData"/> truncates every file back to that point.</item>
/// <item>Directory entries (creates, renames, deletes) become durable only on <see cref="SyncDirectory"/>;
/// <see cref="RevertUnsyncedDirectoryEntries"/> rolls every directory back to its last synced listing
/// (directories that were never synced lose all their entries).</item>
/// <item><see cref="SimulateCrash"/> does both, like FaultInjectionTestFS after a simulated power loss.</item>
/// <item><see cref="SetFilesystemActive"/>(false) makes all writes, syncs and creates fail with IOError.</item>
/// </list>
/// Paths are normalized with <see cref="Path.GetFullPath(string)"/>; no real disk access happens.
/// Thread-safe: metadata is guarded by one lock, file contents by a per-file lock; concurrent reads are allowed.
/// Data written with Append is immediately visible to readers (no separate OS-buffer stage).
/// </summary>
public sealed class MemoryFileSystem : IFileSystem
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, MemFile> _files = new(StringComparer.Ordinal);
    private readonly HashSet<string> _directories = new(StringComparer.Ordinal);
    // Last synced listing per directory: name -> file object.
    private readonly Dictionary<string, Dictionary<string, MemFile>> _durableListings = new(StringComparer.Ordinal);
    private readonly HashSet<string> _lockedFiles = new(StringComparer.Ordinal);
    private volatile bool _active = true;

    private static string Normalize(string path)
    {
        string full = Path.GetFullPath(path);
        if (full.Length > 1 && (full[^1] == Path.DirectorySeparatorChar || full[^1] == Path.AltDirectorySeparatorChar))
            full = full[..^1];
        return full;
    }

    private static string ParentOf(string normalized) => Path.GetDirectoryName(normalized) ?? normalized;

    /// <summary>When false, all mutating operations fail with IOError (FaultInjectionTestFS::SetFilesystemActive).</summary>
    public void SetFilesystemActive(bool active) => _active = active;

    public bool IsFilesystemActive => _active;

    internal void ThrowIfInactive()
    {
        if (!_active) throw new RocksDbException(StatusCode.IOError, "IO error: Filesystem is inactive (injected fault)");
    }

    private static RocksDbException NotFound(string context, string path) =>
        new(StatusCode.IOError, "IO error: " + context + ": " + path + ": No such file or directory", StatusSubCode.PathNotFound);

    private void EnsureParentExists(string normalized)
    {
        string parent = ParentOf(normalized);
        if (parent != normalized && !_directories.Contains(parent) && Path.GetPathRoot(normalized) != parent)
            throw NotFound("While open a file for appending", normalized);
    }

    public IWritableFile CreateWritable(string path, bool truncate = true) => CreateWritable(path, truncate, WritableFileOptions.Default);

    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            EnsureParentExists(p);
            if (!_files.TryGetValue(p, out MemFile? f) || truncate)
            {
                f = new MemFile();
                _files[p] = f;
            }
            return new MemWritableFile(this, f, 0, options.TruncateToSizeOnClose);
        }
    }

    public IWritableFile ReopenWritable(string path, WritableFileOptions options)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            EnsureParentExists(p);
            if (!_files.TryGetValue(p, out MemFile? f))
            {
                f = new MemFile();
                _files[p] = f;
            }
            return new MemWritableFile(this, f, f.Length, options.TruncateToSizeOnClose);
        }
    }

    private MemFile GetFile(string path, string context)
    {
        string p = Normalize(path);
        lock (_lock)
        {
            return _files.TryGetValue(p, out MemFile? f) ? f : throw NotFound(context, path);
        }
    }

    public IRandomAccessFile OpenRandomAccess(string path) => new MemRandomAccessFile(GetFile(path, "While open a file for random read"));

    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) => OpenRandomAccess(path);

    public ISequentialFile OpenSequential(string path) => new MemSequentialFile(GetFile(path, "While opening a file for sequentially reading"));

    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) => OpenSequential(path);

    public bool FileExists(string path)
    {
        string p = Normalize(path);
        lock (_lock) return _files.ContainsKey(p);
    }

    public bool DirectoryExists(string path)
    {
        string p = Normalize(path);
        lock (_lock) return _directories.Contains(p);
    }

    public long GetFileSize(string path) => GetFile(path, "while stat a file for size").Length;

    public void DeleteFile(string path)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock) _files.Remove(p);
    }

    public void RenameFile(string source, string target)
    {
        ThrowIfInactive();
        string s = Normalize(source), t = Normalize(target);
        lock (_lock)
        {
            if (_directories.Contains(s) && !_files.ContainsKey(s))
            {
                RenameDirectoryLocked(s, t);
                return;
            }
            if (!_files.Remove(s, out MemFile? f)) throw NotFound("While renaming a file to " + target, source);
            EnsureParentExists(t);
            _files[t] = f;
        }
    }

    /// <summary>rename(2) of a directory: moves every file and subdirectory below it. Caller holds _lock.</summary>
    private void RenameDirectoryLocked(string s, string t)
    {
        if (_directories.Contains(t) || _files.ContainsKey(t))
            throw new RocksDbException(StatusCode.IOError, "IO error: While renaming a directory to " + t + ": " + s + ": target exists");
        EnsureParentExists(t);
        string prefix = s + Path.DirectorySeparatorChar;
        var dirs = new List<string>();
        foreach (string d in _directories)
            if (d == s || d.StartsWith(prefix, StringComparison.Ordinal)) dirs.Add(d);
        foreach (string d in dirs)
        {
            _directories.Remove(d);
            _directories.Add(t + d[s.Length..]);
        }
        var files = new List<KeyValuePair<string, MemFile>>();
        foreach (KeyValuePair<string, MemFile> kv in _files)
            if (kv.Key.StartsWith(prefix, StringComparison.Ordinal)) files.Add(kv);
        foreach (KeyValuePair<string, MemFile> kv in files)
        {
            _files.Remove(kv.Key);
            _files[t + kv.Key[s.Length..]] = kv.Value;
        }
        var listings = new List<KeyValuePair<string, Dictionary<string, MemFile>>>();
        foreach (KeyValuePair<string, Dictionary<string, MemFile>> kv in _durableListings)
            if (kv.Key == s || kv.Key.StartsWith(prefix, StringComparison.Ordinal)) listings.Add(kv);
        foreach (KeyValuePair<string, Dictionary<string, MemFile>> kv in listings)
        {
            _durableListings.Remove(kv.Key);
            _durableListings[t + kv.Key[s.Length..]] = kv.Value;
        }
    }

    public void CreateDirectoryIfMissing(string path)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            // Create all missing ancestors, like Directory.CreateDirectory.
            string? cur = p;
            while (cur is not null && _directories.Add(cur)) cur = Path.GetDirectoryName(cur);
        }
    }

    public void DeleteDirectory(string path)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            if (!_directories.Contains(p)) throw NotFound("While rmdir", path);
            foreach (string f in _files.Keys)
                if (ParentOf(f) == p) throw new RocksDbException(StatusCode.IOError, "IO error: While rmdir: " + path + ": Directory not empty");
            foreach (string d in _directories)
                if (d != p && ParentOf(d) == p) throw new RocksDbException(StatusCode.IOError, "IO error: While rmdir: " + path + ": Directory not empty");
            _directories.Remove(p);
            _durableListings.Remove(p);
        }
    }

    public IEnumerable<string> GetChildren(string directory)
    {
        string p = Normalize(directory);
        List<string> result = [];
        lock (_lock)
        {
            if (!_directories.Contains(p)) throw NotFound("While opendir", directory);
            foreach (string f in _files.Keys)
                if (ParentOf(f) == p) result.Add(Path.GetFileName(f));
            foreach (string d in _directories)
                if (d != p && ParentOf(d) == p) result.Add(Path.GetFileName(d));
        }
        return result;
    }

    /// <summary>Hard link: both paths name the same file object afterwards (FileSystem::LinkFile).</summary>
    public void LinkFile(string source, string target)
    {
        ThrowIfInactive();
        string s = Normalize(source), t = Normalize(target);
        lock (_lock)
        {
            if (!_files.TryGetValue(s, out MemFile? f)) throw NotFound("while link file to " + target, source);
            EnsureParentExists(t);
            if (_files.ContainsKey(t)) throw new RocksDbException(StatusCode.IOError, "IO error: while link file to " + target + ": " + source + ": File exists");
            _files[t] = f;
        }
    }

    /// <summary>Free space reported by <see cref="GetFreeSpace"/> (default: effectively unlimited). Tests lower it to simulate a full disk.</summary>
    public long FreeSpace { get; set; } = long.MaxValue;

    public long GetFreeSpace(string path) => FreeSpace;
    /// <summary>Random write + sync of an existing file (FSRandomRWFile).</summary>
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data)
    {
        ThrowIfInactive();
        MemFile f = GetFile(path, "While open file for random read/write");
        f.Write(offset, data);
        f.MarkSynced();
    }

    public DateTimeOffset GetFileModificationTime(string path) => GetFile(path, "while stat a file for modification time").LastModified;

    public IDisposable LockFile(string path)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            EnsureParentExists(p);
            if (!_lockedFiles.Add(p))
                throw new RocksDbException(StatusCode.IOError, "IO error: lock " + path + ": already held by process");
            if (!_files.ContainsKey(p)) _files[p] = new MemFile();
        }
        return new MemLock(this, p);
    }

    private void Unlock(string normalized)
    {
        lock (_lock) _lockedFiles.Remove(normalized);
    }

    /// <summary>Makes the current listing of <paramref name="path"/> durable.</summary>
    public void SyncDirectory(string path)
    {
        ThrowIfInactive();
        string p = Normalize(path);
        lock (_lock)
        {
            Dictionary<string, MemFile> listing = new(StringComparer.Ordinal);
            foreach (KeyValuePair<string, MemFile> kv in _files)
                if (ParentOf(kv.Key) == p) listing[Path.GetFileName(kv.Key)] = kv.Value;
            _durableListings[p] = listing;
        }
    }

    /// <summary>Truncates every file to the bytes covered by its last successful sync (DropUnsyncedFileData).</summary>
    public void DropUnsyncedFileData()
    {
        lock (_lock)
        {
            foreach (MemFile f in _files.Values) f.DropUnsynced();
        }
    }

    /// <summary>
    /// Rolls every directory back to its last synced listing (DeleteFilesCreatedAfterLastDirSync + undo of
    /// unsynced renames/deletes). Directories never synced lose all of their entries.
    /// </summary>
    public void RevertUnsyncedDirectoryEntries()
    {
        lock (_lock)
        {
            List<string> toRemove = [];
            foreach (string f in _files.Keys) toRemove.Add(f);
            foreach (string f in toRemove) _files.Remove(f);
            foreach (KeyValuePair<string, Dictionary<string, MemFile>> dir in _durableListings)
            {
                if (!_directories.Contains(dir.Key)) continue;
                foreach (KeyValuePair<string, MemFile> entry in dir.Value)
                    _files[Path.Combine(dir.Key, entry.Key)] = entry.Value;
            }
            _lockedFiles.Clear();
        }
    }

    /// <summary>
    /// Simulates a power loss: unsynced directory entries and unsynced file data are lost. Open file objects must
    /// not be used afterwards.
    /// </summary>
    public void SimulateCrash()
    {
        RevertUnsyncedDirectoryEntries();
        DropUnsyncedFileData();
    }

    /// <summary>Returns a copy of a file's current contents (test helper).</summary>
    public byte[] ReadAllBytes(string path)
    {
        MemFile f = GetFile(path, "While reading");
        lock (f.Lock) return f.Data.AsSpan(0, (int)f.Length).ToArray();
    }

    internal sealed class MemFile
    {
        public readonly Lock Lock = new();
        public byte[] Data = [];
        public DateTimeOffset LastModified = DateTimeOffset.UtcNow;
        private long _length;
        private long _synced;

        public long Length
        {
            get { lock (Lock) return _length; }
        }

        public void Write(long offset, ReadOnlySpan<byte> data)
        {
            lock (Lock)
            {
                long end = offset + data.Length;
                if (end > Data.Length)
                {
                    long newSize = Math.Max(end, Math.Max(Data.Length * 2L, 256));
                    if (newSize > Array.MaxLength) newSize = Math.Max(end, Array.MaxLength);
                    Array.Resize(ref Data, (int)newSize);
                }
                if (offset > _length) Data.AsSpan((int)_length, (int)(offset - _length)).Clear();
                data.CopyTo(Data.AsSpan((int)offset));
                if (end > _length) _length = end;
                LastModified = DateTimeOffset.UtcNow;
            }
        }

        public int Read(long offset, Span<byte> destination)
        {
            lock (Lock)
            {
                if (offset >= _length) return 0;
                int n = (int)Math.Min(destination.Length, _length - offset);
                Data.AsSpan((int)offset, n).CopyTo(destination);
                return n;
            }
        }

        /// <summary>ftruncate(length) when the file is longer (the cut is durable, like the data it removes).</summary>
        public void Truncate(long length)
        {
            lock (Lock)
            {
                if (length >= _length) return;
                _length = length;
                if (_synced > length) _synced = length;
                LastModified = DateTimeOffset.UtcNow;
            }
        }

        public void MarkSynced()
        {
            lock (Lock) _synced = _length;
        }

        public void DropUnsynced()
        {
            lock (Lock)
            {
                if (_synced < _length) _length = _synced;
            }
        }
    }

    private sealed class MemLock(MemoryFileSystem fs, string path) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) fs.Unlock(path);
        }
    }

    private sealed class MemWritableFile(MemoryFileSystem fs, MemFile file, long position, bool truncateOnClose) : IWritableFile
    {
        private long _position = position;
        private bool _disposed;

        public long Size => _position;

        public void Append(ReadOnlySpan<byte> data)
        {
            if (_disposed) ThrowHelper.InvalidArgument("file already closed");
            fs.ThrowIfInactive();
            file.Write(_position, data);
            _position += data.Length;
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
            if (!_disposed) fs.ThrowIfInactive();
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
            if (_disposed) ThrowHelper.InvalidArgument("file already closed");
            fs.ThrowIfInactive();
            file.MarkSynced();
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
            if (_disposed) return;
            _disposed = true;
            // PosixWritableFile::Close: ftruncate to the written size (no-op once the file system is "dead").
            if (truncateOnClose && fs.IsFilesystemActive) file.Truncate(_position);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class MemRandomAccessFile(MemFile file) : IRandomAccessFile
    {
        public long Length => file.Length;

        public int Read(long offset, Span<byte> destination) => file.Read(offset, destination);

        public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(file.Read(offset, destination.Span));

        public void Dispose() { }
    }

    private sealed class MemSequentialFile(MemFile file) : ISequentialFile
    {
        private long _position;

        public int Read(Span<byte> destination)
        {
            int n = file.Read(_position, destination);
            _position += n;
            return n;
        }

        public ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default) =>
            new(Read(destination.Span));

        public void Skip(long bytes) => _position += bytes;

        public void Dispose() { }
    }
}
