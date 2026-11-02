using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Env;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication;

/// <summary>
/// One file of a <see cref="ReplicationSession"/> (a checkpoint of the source database), opened for reading.
/// Port of rocksdb-sharp's <c>ReplicationFile</c>; reads go through the database's <see cref="IFileSystem"/>
/// (positional, thread-safe) instead of a <c>FileStream</c>, so a source on a <see cref="MemoryFileSystem"/> works too.
/// <see cref="Stream"/> exposes the content as a read-only <see cref="System.IO.Stream"/> for transports that want one.
/// </summary>
public sealed class ReplicationFile : IDisposable
{
    private readonly IRandomAccessFile _file;
    private Stream? _stream;

    internal ReplicationFile(string fileName, IRandomAccessFile file)
    {
        FileName = fileName;
        _file = file;
        FileSize = (ulong)file.Length;
    }

    public string FileName { get; }
    public ulong FileSize { get; }

    /// <summary>A read-only, seekable stream over the file (created on first use, disposed with this object).</summary>
    public Stream Stream => _stream ??= new RandomAccessFileStream(_file);

    /// <summary>Reads up to <paramref name="destination"/>.Length bytes at <paramref name="offset"/>; short only at EOF.</summary>
    public ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
        _file.ReadAsync(offset, destination, cancellationToken);

    /// <summary>Copies the whole file to <paramref name="destination"/>.</summary>
    public async ValueTask CopyToAsync(IWritableFile destination, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(destination);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(256 * 1024);
        try
        {
            long offset = 0;
            while (true)
            {
                int n = await _file.ReadAsync(offset, buffer, cancellationToken).ConfigureAwait(false);
                if (n <= 0) break;
                await destination.AppendAsync(buffer.AsMemory(0, n), cancellationToken).ConfigureAwait(false);
                offset += n;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _file.Dispose();
    }

    private sealed class RandomAccessFileStream(IRandomAccessFile file) : Stream
    {
        private long _position;

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => file.Length;

        public override long Position
        {
            get => _position;
            set => _position = value;
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            int n = file.Read(_position, buffer);
            _position += n;
            return n;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int n = await file.ReadAsync(_position, buffer, cancellationToken).ConfigureAwait(false);
            _position += n;
            return n;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override long Seek(long offset, SeekOrigin origin)
        {
            _position = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                _ => file.Length + offset,
            };
            return _position;
        }

        public override void Flush()
        {
        }

        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}

/// <summary>
/// Inventory entry describing one database file on either side of a snapshot transfer (rocksdb-sharp
/// <c>ReplicationFileInfo</c>).
/// </summary>
public sealed class ReplicationFileInfo
{
    public required string FileName { get; init; }
    public required long Size { get; init; }

    /// <summary>Identity signature (see <see cref="ReplicationDelta.ComputeFileSignatureAsync(IFileSystem, string, CancellationToken)"/>). Empty when not computed.</summary>
    public string Hash { get; init; } = string.Empty;
}

/// <summary>What a <see cref="SnapshotChunk"/> carries.</summary>
public enum SnapshotChunkKind
{
    /// <summary>The first message of a snapshot stream: which local files the consumer keeps (<see cref="SnapshotChunk.KeepFiles"/>).</summary>
    Plan = 0,
    /// <summary>A piece of one file (<see cref="SnapshotChunk.FileName"/> at <see cref="SnapshotChunk.Offset"/>).</summary>
    FileData = 1,
    /// <summary>
    /// The last message: the snapshot is complete (<see cref="SnapshotChunk.FileSize"/> holds the number of transferred
    /// files). A stream that ends without it was cut off and must not be opened as a database.
    /// </summary>
    Complete = 2,
}

/// <summary>
/// One message of a snapshot transfer produced by <see cref="ReplicationSession.StreamSnapshotAsync"/> and consumed
/// by <see cref="SnapshotReceiver"/>. The transport is up to the host (the rocksdb-sharp samples used a gRPC server
/// stream with the same shape: a plan, then file chunks, CURRENT last).
/// <para><see cref="Content"/> refers to a buffer owned by the producer and is only valid until the producing
/// enumeration moves on: a transport that queues messages must copy it.</para>
/// </summary>
public readonly struct SnapshotChunk
{
    private SnapshotChunk(SnapshotChunkKind kind, ulong sequenceNumber, IReadOnlyList<string>? keepFiles, string? fileName, long fileSize, long offset,
        ReadOnlyMemory<byte> content)
    {
        Kind = kind;
        SequenceNumber = sequenceNumber;
        KeepFiles = keepFiles ?? [];
        FileName = fileName;
        FileSize = fileSize;
        Offset = offset;
        Content = content;
    }

    public static SnapshotChunk Plan(ulong sequenceNumber, IReadOnlyList<string> keepFiles) =>
        new(SnapshotChunkKind.Plan, sequenceNumber, keepFiles, null, 0, 0, default);

    public static SnapshotChunk FileData(string fileName, long fileSize, long offset, ReadOnlyMemory<byte> content) =>
        new(SnapshotChunkKind.FileData, 0, null, fileName, fileSize, offset, content);

    public static SnapshotChunk Complete(int transferredFiles) =>
        new(SnapshotChunkKind.Complete, 0, null, null, transferredFiles, 0, default);

    public SnapshotChunkKind Kind { get; }

    /// <summary>Plan only: the sequence number the source's checkpoint covers (informational; the consumer resumes from its own latest).</summary>
    public ulong SequenceNumber { get; }

    /// <summary>Plan only: the consumer's verified files that are reused and not transferred.</summary>
    public IReadOnlyList<string> KeepFiles { get; }

    public string? FileName { get; }
    public long FileSize { get; }
    public long Offset { get; }
    public ReadOnlyMemory<byte> Content { get; }

    /// <summary>A copy whose <see cref="Content"/> no longer refers to the producer's buffer.</summary>
    public SnapshotChunk Detach() =>
        Kind != SnapshotChunkKind.FileData ? this : new(Kind, SequenceNumber, KeepFiles, FileName, FileSize, Offset, Content.ToArray());
}

/// <summary>
/// A checkpoint of the source database, ready to be shipped to a replica (rocksdb-sharp <c>ReplicationSession</c>).
/// Created by <see cref="ReplicationSource.GetInitialStateAsync"/>.
/// <para>
/// While the session holds its file-deletion pin (until <see cref="ReleaseFileDeletions"/> or disposal), the source
/// keeps every WAL - so the replica can start streaming from the end of the checkpoint without racing a WAL purge -
/// and every obsolete SST. Release it once the replica is attached to the WAL stream, or dispose the session.
/// Disposing deletes the checkpoint directory.
/// </para>
/// </summary>
public sealed class ReplicationSession : IDisposable
{
    private readonly ReplicationSource? _source;
    private int _pinned;
    private int _disposed;

    internal ReplicationSession(IFileSystem fileSystem, string directory, ulong sequenceNumber, ReplicationSource? source)
    {
        FileSystem = fileSystem;
        Directory = directory;
        SequenceNumber = sequenceNumber;
        _source = source;
        _pinned = source is null ? 0 : 1;
    }

    /// <summary>Opens a session over an existing checkpoint directory (no file-deletion pin).</summary>
    public static ReplicationSession FromDirectory(IFileSystem fileSystem, string directory, ulong sequenceNumber)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(directory);
        return new ReplicationSession(fileSystem, directory, sequenceNumber, null);
    }

    public IFileSystem FileSystem { get; }

    /// <summary>The checkpoint directory.</summary>
    public string Directory { get; }

    /// <summary>
    /// The source's latest sequence number when the checkpoint started: every write up to it is in the checkpoint
    /// (concurrent later writes may be too). A replica opened from the checkpoint resumes from its own
    /// <c>GetLatestSequenceNumber() + 1</c>.
    /// </summary>
    public ulong SequenceNumber { get; }

    /// <summary>The checkpoint's file names in transfer order (CURRENT last, see <see cref="OrderForTransfer"/>).</summary>
    public IReadOnlyList<string> GetFileNames()
    {
        var names = new List<string>();
        foreach (string child in FileSystem.GetChildren(Directory))
        {
            if (FileSystem.FileExists(FileSystemHelpers.Combine(Directory, child))) names.Add(child);
        }
        return OrderForTransfer(names);
    }

    /// <summary>
    /// Sorts file names so that CURRENT comes last: it names the live MANIFEST, so a consumer that dies mid-transfer
    /// is left without a CURRENT marker and cleanly re-pulls instead of opening a half-restored database.
    /// </summary>
    public static IReadOnlyList<string> OrderForTransfer(IEnumerable<string> names)
    {
        var list = new List<string>(names);
        list.Sort(static (a, b) =>
        {
            int ca = a == "CURRENT" ? 1 : 0, cb = b == "CURRENT" ? 1 : 0;
            return ca != cb ? ca.CompareTo(cb) : string.CompareOrdinal(a, b);
        });
        return list;
    }

    /// <summary>Opens every checkpoint file in transfer order. The caller disposes each file.</summary>
    public IEnumerable<ReplicationFile> Files
    {
        get
        {
            foreach (string name in GetFileNames()) yield return OpenFile(name);
        }
    }

    /// <summary>
    /// Lists the checkpoint's files with their sizes - the source-side half of a per-file delta transfer. Hashes are
    /// not computed here; candidates are verified on demand (<see cref="ReplicationDelta.TryHashCandidateAsync(IFileSystem, string, string, long, CancellationToken)"/>).
    /// </summary>
    public List<ReplicationFileInfo> GetManifest()
    {
        var result = new List<ReplicationFileInfo>();
        foreach (string name in GetFileNames())
        {
            result.Add(new ReplicationFileInfo { FileName = name, Size = FileSystem.GetFileSize(FileSystemHelpers.Combine(Directory, name)) });
        }
        return result;
    }

    public ReplicationFile OpenFile(string fileName)
    {
        if (!ReplicationDelta.IsPlainFileName(fileName)) throw new ArgumentException("not a plain file name: " + fileName, nameof(fileName));
        return new ReplicationFile(fileName, FileSystem.OpenRandomAccess(FileSystemHelpers.Combine(Directory, fileName)));
    }

    /// <summary>
    /// Produces the snapshot stream for a consumer: a <see cref="SnapshotChunkKind.Plan"/> message (computed with
    /// <see cref="ReplicationDelta.Compute"/> from <paramref name="consumerInventory"/> - pass an empty list for a full
    /// transfer), then every file that is not reused, in chunks of at most <paramref name="chunkSize"/> bytes, CURRENT
    /// last. Every transferred file produces at least one chunk (an empty one for an empty file); a
    /// <see cref="SnapshotChunkKind.Complete"/> message ends the stream. Each chunk's content is valid until the enumeration
    /// moves on.
    /// </summary>
    public async IAsyncEnumerable<SnapshotChunk> StreamSnapshotAsync(IReadOnlyCollection<ReplicationFileInfo> consumerInventory, int chunkSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(consumerInventory);
        ArgumentOutOfRangeException.ThrowIfLessThan(chunkSize, 1);
        ReplicationDeltaPlan plan = ReplicationDelta.Compute(GetManifest(), consumerInventory);
        yield return SnapshotChunk.Plan(SequenceNumber, plan.FilesToReuse);
        int transferred = 0;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
        try
        {
            foreach (string name in OrderForTransfer(plan.FilesToTransfer))
            {
                using ReplicationFile file = OpenFile(name);
                transferred++;
                long offset = 0;
                bool sentAny = false;
                while (true)
                {
                    int n = await file.ReadAsync(offset, buffer.AsMemory(0, chunkSize), cancellationToken).ConfigureAwait(false);
                    if (n <= 0) break;
                    yield return SnapshotChunk.FileData(name, (long)file.FileSize, offset, buffer.AsMemory(0, n));
                    offset += n;
                    sentAny = true;
                }
                if (!sentAny) yield return SnapshotChunk.FileData(name, 0, 0, ReadOnlyMemory<byte>.Empty);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        yield return SnapshotChunk.Complete(transferred);
    }

    /// <summary>
    /// Lets the source delete obsolete files again (undoes the <see cref="RocksDatabase.DisableFileDeletions"/> taken
    /// for the checkpoint). Idempotent; also done by <see cref="Dispose"/>.
    /// </summary>
    public void ReleaseFileDeletions()
    {
        if (Interlocked.Exchange(ref _pinned, 0) == 1) _source!.OnSessionReleased();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        try
        {
            FileSystemHelpers.DeleteDirectoryRecursive(FileSystem, Directory);
        }
        finally
        {
            ReleaseFileDeletions();
        }
    }
}
