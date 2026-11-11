using System.Buffers;

namespace RocksDb.Managed.Env;

/// <summary>
/// Thrown by <see cref="MirrorFileSystem"/> when the two mirrored file systems disagree on the result of an operation
/// (upstream's EnvMirror asserts instead).
/// </summary>
public sealed class FileSystemMirrorMismatchException : Exception
{
    /// <summary>Creates the exception.</summary>
    public FileSystemMirrorMismatchException(string message, Exception? inner = null) : base(message, inner) { }
}

/// <summary>
/// A file system that performs every operation on two file systems and checks that both give the same result: the
/// same return values, the same data read back, and either both succeeding or both failing. Results are those of
/// <see cref="A"/>; a disagreement throws <see cref="FileSystemMirrorMismatchException"/>. Port of EnvMirror
/// (include/rocksdb/utilities/env_mirror.h, utilities/env_mirror.cc), a testing utility: run a workload over e.g.
/// <see cref="LocalFileSystem"/> mirrored with <see cref="MemoryFileSystem"/> to check that the two behave alike.
/// </summary>
/// <remarks>
/// As upstream, a few queries are not verified and come from <see cref="A"/> only: free space, file modification
/// times and readahead hints (<see cref="IRandomAccessFile.Prefetch"/>). When both sides fail, the exception of
/// <see cref="A"/> is rethrown (the two exceptions may differ in type and message). Thread-safe if both file systems
/// are; mirrored files are as thread-safe as their two underlying files.
/// </remarks>
public sealed class MirrorFileSystem : IFileSystem
{
    /// <summary>Creates a mirror of <paramref name="a"/> (whose results are returned) and <paramref name="b"/>.</summary>
    public MirrorFileSystem(IFileSystem a, IFileSystem b)
    {
        ArgumentNullException.ThrowIfNull(a);
        ArgumentNullException.ThrowIfNull(b);
        A = a;
        B = b;
    }

    /// <summary>The primary file system (its results are returned).</summary>
    public IFileSystem A { get; }

    /// <summary>The mirror file system.</summary>
    public IFileSystem B { get; }

    // ---- helpers ------------------------------------------------------------------------------------------------

    internal static Exception Mismatch(string what, Exception? inner = null) =>
        new FileSystemMirrorMismatchException("EnvMirror: results differ for " + what, inner);

    /// <summary>Runs <paramref name="a"/> and <paramref name="b"/>; both must succeed (returning equal values) or both fail.</summary>
    private static T Both<T>(string what, Func<T> a, Func<T> b, Func<T, T, bool>? equal = null, Action<T>? discardB = null)
    {
        T ra;
        try
        {
            ra = a();
        }
        catch (Exception ea) when (ea is not FileSystemMirrorMismatchException)
        {
            T rb;
            try
            {
                rb = b();
            }
            catch (Exception eb) when (eb is not FileSystemMirrorMismatchException)
            {
                // Both failed: consistent; report the first file system's error.
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(ea);
                throw;
            }
            discardB?.Invoke(rb);
            throw Mismatch(what + ": only the first file system failed", ea);
        }
        T rbOk;
        try
        {
            rbOk = b();
        }
        catch (Exception eb) when (eb is not FileSystemMirrorMismatchException)
        {
            (ra as IDisposable)?.Dispose();
            throw Mismatch(what + ": only the second file system failed", eb);
        }
        if (equal is not null && !equal(ra, rbOk))
        {
            discardB?.Invoke(rbOk);
            throw Mismatch(what + ": " + ra + " vs " + rbOk);
        }
        return ra;
    }

    private static void Both(string what, Action a, Action b) =>
        Both<int>(what, () => { a(); return 0; }, () => { b(); return 0; });

    private static string P(string op, string path) => op + "(" + path + ")";

    // ---- files ----------------------------------------------------------------------------------------------------

    /// <inheritdoc/>
    public IWritableFile CreateWritable(string path, bool truncate = true) =>
        CreatePair(P("CreateWritable", path), () => A.CreateWritable(path, truncate), () => B.CreateWritable(path, truncate), path);

    /// <inheritdoc/>
    public IWritableFile CreateWritable(string path, bool truncate, WritableFileOptions options) =>
        CreatePair(P("CreateWritable", path), () => A.CreateWritable(path, truncate, options), () => B.CreateWritable(path, truncate, options), path);

    /// <inheritdoc/>
    public IWritableFile ReopenWritable(string path, WritableFileOptions options) =>
        CreatePair(P("ReopenWritable", path), () => A.ReopenWritable(path, options), () => B.ReopenWritable(path, options), path);

    /// <inheritdoc/>
    public IWritableFile ReuseWritable(string path, string oldPath, WritableFileOptions options) =>
        CreatePair(P("ReuseWritable", path), () => A.ReuseWritable(path, oldPath, options), () => B.ReuseWritable(path, oldPath, options), path);

    private static IWritableFile CreatePair(string what, Func<IWritableFile> a, Func<IWritableFile> b, string path)
    {
        IWritableFile? fb = null;
        IWritableFile fa = Both(what, a, () => fb = b(), null, static f => f.Dispose());
        return new MirrorWritableFile(fa, fb!, path);
    }

    /// <inheritdoc/>
    public IRandomAccessFile OpenRandomAccess(string path) =>
        OpenRandomPair(P("OpenRandomAccess", path), () => A.OpenRandomAccess(path), () => B.OpenRandomAccess(path), path);

    /// <inheritdoc/>
    public IRandomAccessFile OpenRandomAccess(string path, RandomAccessFileOptions options) =>
        OpenRandomPair(P("OpenRandomAccess", path), () => A.OpenRandomAccess(path, options), () => B.OpenRandomAccess(path, options), path);

    private static IRandomAccessFile OpenRandomPair(string what, Func<IRandomAccessFile> a, Func<IRandomAccessFile> b, string path)
    {
        IRandomAccessFile? fb = null;
        IRandomAccessFile fa = Both(what, a, () => fb = b(), null, static f => f.Dispose());
        return new MirrorRandomAccessFile(fa, fb!, path);
    }

    /// <inheritdoc/>
    public ISequentialFile OpenSequential(string path) =>
        OpenSequentialPair(P("OpenSequential", path), () => A.OpenSequential(path), () => B.OpenSequential(path), path);

    /// <inheritdoc/>
    public ISequentialFile OpenSequential(string path, SequentialFileOptions options) =>
        OpenSequentialPair(P("OpenSequential", path), () => A.OpenSequential(path, options), () => B.OpenSequential(path, options), path);

    private static ISequentialFile OpenSequentialPair(string what, Func<ISequentialFile> a, Func<ISequentialFile> b, string path)
    {
        ISequentialFile? fb = null;
        ISequentialFile fa = Both(what, a, () => fb = b(), null, static f => f.Dispose());
        return new MirrorSequentialFile(fa, fb!, path);
    }

    // ---- metadata -------------------------------------------------------------------------------------------------

    /// <inheritdoc/>
    public bool FileExists(string path) => Both(P("FileExists", path), () => A.FileExists(path), () => B.FileExists(path), static (x, y) => x == y);

    /// <inheritdoc/>
    public long GetFileSize(string path) => Both(P("GetFileSize", path), () => A.GetFileSize(path), () => B.GetFileSize(path), static (x, y) => x == y);

    /// <inheritdoc/>
    public void DeleteFile(string path) => Both(P("DeleteFile", path), () => A.DeleteFile(path), () => B.DeleteFile(path));

    /// <inheritdoc/>
    public void RenameFile(string source, string target) =>
        Both(P("RenameFile", source + " -> " + target), () => A.RenameFile(source, target), () => B.RenameFile(source, target));

    /// <inheritdoc/>
    public void CreateDirectoryIfMissing(string path) =>
        Both(P("CreateDirectoryIfMissing", path), () => A.CreateDirectoryIfMissing(path), () => B.CreateDirectoryIfMissing(path));

    /// <summary>GetChildren: the sorted listings must be equal (EnvMirror::GetChildren).</summary>
    public IEnumerable<string> GetChildren(string directory)
    {
        List<string> la = Both(P("GetChildren", directory), () => Sorted(A.GetChildren(directory)), () => Sorted(B.GetChildren(directory)),
            static (x, y) => x.Count == y.Count && x.SequenceEqual(y, StringComparer.Ordinal));
        return la;
    }

    private static List<string> Sorted(IEnumerable<string> names)
    {
        var l = new List<string>(names);
        l.Sort(StringComparer.Ordinal);
        return l;
    }

    /// <inheritdoc/>
    public IDisposable LockFile(string path)
    {
        IDisposable? lb = null;
        IDisposable la = Both(P("LockFile", path), () => A.LockFile(path), () => lb = B.LockFile(path), null, static l => l.Dispose());
        return new MirrorLock(la, lb!);
    }

    /// <inheritdoc/>
    public void SyncDirectory(string path) => Both(P("SyncDirectory", path), () => A.SyncDirectory(path), () => B.SyncDirectory(path));

    /// <inheritdoc/>
    public bool DirectoryExists(string path) =>
        Both(P("DirectoryExists", path), () => A.DirectoryExists(path), () => B.DirectoryExists(path), static (x, y) => x == y);

    /// <inheritdoc/>
    public void DeleteDirectory(string path) => Both(P("DeleteDirectory", path), () => A.DeleteDirectory(path), () => B.DeleteDirectory(path));

    /// <inheritdoc/>
    public void LinkFile(string source, string target) =>
        Both(P("LinkFile", source + " -> " + target), () => A.LinkFile(source, target), () => B.LinkFile(source, target));

    /// <inheritdoc/>
    public void WriteAt(string path, long offset, ReadOnlySpan<byte> data)
    {
        byte[] copy = data.ToArray();
        Both(P("WriteAt", path), () => A.WriteAt(path, offset, copy), () => B.WriteAt(path, offset, copy));
    }

    /// <summary>The modification time of <see cref="A"/> (not verified, like upstream's tolerance check).</summary>
    public DateTimeOffset GetFileModificationTime(string path) =>
        Both(P("GetFileModificationTime", path), () => A.GetFileModificationTime(path), () => B.GetFileModificationTime(path));

    /// <summary>The free space of <see cref="A"/> (not verified: the two file systems have different capacities).</summary>
    public long GetFreeSpace(string path) => A.GetFreeSpace(path);

    // ---- mirrored files ---------------------------------------------------------------------------------------------

    private sealed class MirrorLock(IDisposable a, IDisposable b) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            Both("UnlockFile", a.Dispose, b.Dispose);
        }
    }

    /// <summary>WritableFileMirror.</summary>
    private sealed class MirrorWritableFile(IWritableFile a, IWritableFile b, string path) : IWritableFile
    {
        public long Size
        {
            get
            {
                long sa = a.Size, sb = b.Size;
                if (sa != sb) throw Mismatch(P("Size", path) + ": " + sa + " vs " + sb);
                return sa;
            }
        }

        public void Append(ReadOnlySpan<byte> data)
        {
            byte[] copy = data.ToArray();
            Both(P("Append", path), () => a.Append(copy), () => b.Append(copy));
        }

        public async ValueTask AppendAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            await BothAsync(P("AppendAsync", path), () => a.AppendAsync(data, cancellationToken), () => b.AppendAsync(data, cancellationToken)).ConfigureAwait(false);
        }

        public ValueTask FlushAsync(CancellationToken cancellationToken = default) =>
            BothAsync(P("FlushAsync", path), () => a.FlushAsync(cancellationToken), () => b.FlushAsync(cancellationToken));

        public ValueTask SyncAsync(CancellationToken cancellationToken = default) =>
            BothAsync(P("SyncAsync", path), () => a.SyncAsync(cancellationToken), () => b.SyncAsync(cancellationToken));

        public void Flush() => Both(P("Flush", path), a.Flush, b.Flush);

        public void Sync() => Both(P("Sync", path), a.Sync, b.Sync);

        public void Dispose() => Both(P("Close", path), a.Dispose, b.Dispose);

        public async ValueTask DisposeAsync()
        {
            await BothAsync(P("CloseAsync", path), a.DisposeAsync, b.DisposeAsync).ConfigureAwait(false);
        }
    }

    private static async ValueTask BothAsync(string what, Func<ValueTask> a, Func<ValueTask> b)
    {
        Exception? ea = null, eb = null;
        try
        {
            await a().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not FileSystemMirrorMismatchException)
        {
            ea = e;
        }
        try
        {
            await b().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not FileSystemMirrorMismatchException)
        {
            eb = e;
        }
        if (ea is not null && eb is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(ea);
        if (ea is not null) throw Mismatch(what + ": only the first file system failed", ea);
        if (eb is not null) throw Mismatch(what + ": only the second file system failed", eb);
    }

    /// <summary>RandomAccessFileMirror: reads both files and compares the bytes.</summary>
    private sealed class MirrorRandomAccessFile(IRandomAccessFile a, IRandomAccessFile b, string path) : IRandomAccessFile
    {
        public long Length
        {
            get
            {
                long la = a.Length, lb = b.Length;
                if (la != lb) throw Mismatch(P("Length", path) + ": " + la + " vs " + lb);
                return la;
            }
        }

        public int Read(long offset, Span<byte> destination)
        {
            int na = a.Read(offset, destination);
            byte[] scratch = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, na));
            try
            {
                // RandomAccessFileMirror::Read: the bytes the first file returned are read from the second one, which may
                // deliver them in several short reads ("while (left)").
                int nb = 0;
                while (nb < na)
                {
                    int r = b.Read(offset + nb, scratch.AsSpan(nb, na - nb));
                    if (r <= 0) break;
                    nb += r;
                }
                Compare(offset, destination[..na], scratch.AsSpan(0, nb));
            }
            finally
            {
                RocksArrayPool<byte>.Shared.Return(scratch);
            }
            return na;
        }

        public async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            int na = await a.ReadAsync(offset, destination, cancellationToken).ConfigureAwait(false);
            byte[] scratch = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, na));
            try
            {
                int nb = 0;
                while (nb < na)
                {
                    int r = await b.ReadAsync(offset + nb, scratch.AsMemory(nb, na - nb), cancellationToken).ConfigureAwait(false);
                    if (r <= 0) break;
                    nb += r;
                }
                Compare(offset, destination.Span[..na], scratch.AsSpan(0, nb));
            }
            finally
            {
                RocksArrayPool<byte>.Shared.Return(scratch);
            }
            return na;
        }

        private void Compare(long offset, ReadOnlySpan<byte> ra, ReadOnlySpan<byte> rb)
        {
            if (!ra.SequenceEqual(rb)) throw Mismatch(P("Read", path) + " at " + offset + ": " + ra.Length + " vs " + rb.Length + " bytes, or different data");
        }

        /// <summary>Readahead hint: forwarded to both, the result of <c>A</c> (not verified).</summary>
        public bool Prefetch(long offset, long length)
        {
            b.Prefetch(offset, length);
            return a.Prefetch(offset, length);
        }

        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }

    /// <summary>SequentialFileMirror: reads both files and compares the bytes.</summary>
    private sealed class MirrorSequentialFile(ISequentialFile a, ISequentialFile b, string path) : ISequentialFile
    {
        private long _position;

        public int Read(Span<byte> destination)
        {
            int na = a.Read(destination);
            byte[] scratch = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, na));
            try
            {
                // Read the same number of bytes from b (it may return them in several short reads).
                int nb = 0;
                while (nb < na)
                {
                    int r = b.Read(scratch.AsSpan(nb, na - nb));
                    if (r <= 0) break;
                    nb += r;
                }
                if (nb != na || !destination[..na].SequenceEqual(scratch.AsSpan(0, nb)))
                {
                    throw Mismatch(P("SequentialRead", path) + " at " + _position + ": " + na + " vs " + nb + " bytes, or different data");
                }
            }
            finally
            {
                RocksArrayPool<byte>.Shared.Return(scratch);
            }
            _position += na;
            return na;
        }

        public async ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            int na = await a.ReadAsync(destination, cancellationToken).ConfigureAwait(false);
            byte[] scratch = RocksArrayPool<byte>.Shared.Rent(Math.Max(1, na));
            try
            {
                int nb = 0;
                while (nb < na)
                {
                    int r = await b.ReadAsync(scratch.AsMemory(nb, na - nb), cancellationToken).ConfigureAwait(false);
                    if (r <= 0) break;
                    nb += r;
                }
                if (nb != na || !destination.Span[..na].SequenceEqual(scratch.AsSpan(0, nb)))
                {
                    throw Mismatch(P("SequentialRead", path) + " at " + _position + ": " + na + " vs " + nb + " bytes, or different data");
                }
            }
            finally
            {
                RocksArrayPool<byte>.Shared.Return(scratch);
            }
            _position += na;
            return na;
        }

        public void Skip(long bytes)
        {
            Both(P("Skip", path), () => a.Skip(bytes), () => b.Skip(bytes));
            _position += bytes;
        }

        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }
}
