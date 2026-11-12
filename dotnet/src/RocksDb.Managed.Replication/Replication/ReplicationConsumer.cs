using System.Runtime.CompilerServices;
using RocksDb.Managed.Replication.Internal;

namespace RocksDb.Managed.Replication;

/// <summary>
/// The replica side of replication (port of rocksdb-sharp <c>ReplicationConsumer</c>): applies the source's WAL batches
/// or merged <see cref="WalChunk"/>s as single writes. Because a replica assigns sequence numbers itself (latest + 1),
/// applying every batch in order with no other writes keeps it on exactly the source's sequence numbers;
/// <see cref="TryIngestChunkAsync"/> checks that before applying.
/// <para>Managed differences: async, and the bytes are copied into a pooled <see cref="WriteBatch"/> instead of
/// constructing a native batch from a <c>byte[]</c> per message.</para>
/// </summary>
public sealed class ReplicationConsumer
{
    /// <summary>The default <see cref="ApplyParallelism"/>: the processor count, at most 4.</summary>
    public static int DefaultApplyParallelism => Math.Clamp(Environment.ProcessorCount, 1, 4);

    private readonly RocksDatabase _db;
    private readonly WriteOptions _writeOptions = new() { MemtableInsertParallelism = DefaultApplyParallelism };

    public ReplicationConsumer(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        _db = db;
    }

    public RocksDatabase Database => _db;

    /// <summary>
    /// How many threads insert one applied chunk into the replica's memtable
    /// (<see cref="WriteOptions.MemtableInsertParallelism"/>; default <see cref="DefaultApplyParallelism"/>). The primary's
    /// concurrent writers insert their batches in parallel; a replica applies the merged chunk as one write, so a serial
    /// insert caps it below a primary with several writers. 1 restores the serial insert. Chunks are still applied one at
    /// a time, in order, each as one atomic write at the source's sequence numbers. Set it before applying.
    /// </summary>
    public int ApplyParallelism
    {
        get => _writeOptions.MemtableInsertParallelism;
        set => _writeOptions.MemtableInsertParallelism = value;
    }

    /// <summary>
    /// Whether every applied chunk is fsync'd before the write returns (and before the replica's latest sequence number
    /// moves past it). A Raft follower must not report an entry it could lose in a power loss. Off by default (a plain
    /// replica relies on the source for durability). Set it before applying.
    /// </summary>
    public bool SyncWrites
    {
        get => _writeOptions.Sync;
        set => _writeOptions.Sync = value;
    }

    /// <summary>The sequence number the next applied batch must start with (latest + 1).</summary>
    public ulong ExpectedNextSequence => _db.GetLatestSequenceNumber() + 1;

    /// <summary>Applies one serialized write batch - a WAL batch or a merged chunk - as one write. Allocation-free in steady state.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    public async ValueTask IngestChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken)
    {
        WriteBatch batch = WriteBatch.Rent();
        try
        {
            batch.SetContents(chunk.Span);
            await _db.WriteAsync(_writeOptions, batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            batch.Dispose();
        }
    }

    /// <summary>Synchronous <see cref="IngestChunkAsync"/>.</summary>
    public void IngestChunk(ReadOnlySpan<byte> chunk)
    {
        using WriteBatch batch = WriteBatch.Rent();
        batch.SetContents(chunk);
        _db.Write(_writeOptions, batch);
    }

    public ValueTask IngestBatchAsync(WalBatch batch, CancellationToken cancellationToken) => IngestChunkAsync(batch.Data, cancellationToken);

    /// <summary>
    /// Applies <paramref name="chunk"/> only if it starts exactly at <see cref="ExpectedNextSequence"/>; returns false
    /// (and writes nothing) otherwise. A gap means the source no longer has the WAL the replica needs; an overlap means
    /// something else wrote to the replica. Either way the replica diverged and must be rebuilt from a checkpoint. The
    /// check assumes this consumer is the replica's only writer.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<bool> TryIngestChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken)
    {
        if (new WalBatch(chunk).Sequence != ExpectedNextSequence) return false;
        await IngestChunkAsync(chunk, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>Copies a checkpoint file into <paramref name="destinationDbPath"/> on the local file system.</summary>
    public static ValueTask IngestFileAsync(ReplicationFile file, string destinationDbPath, CancellationToken cancellationToken) =>
        IngestFileAsync(file, LocalFileSystem.Instance, destinationDbPath, cancellationToken);

    /// <summary>Copies a checkpoint file into <paramref name="destinationDbPath"/> (created if missing) and syncs it.</summary>
    public static async ValueTask IngestFileAsync(ReplicationFile file, IFileSystem fileSystem, string destinationDbPath, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(fileSystem);
        fileSystem.CreateDirectoryIfMissing(destinationDbPath);
        await using IWritableFile dst = fileSystem.CreateWritable(FileSystemHelpers.Combine(destinationDbPath, file.FileName), truncate: true);
        await file.CopyToAsync(dst, cancellationToken).ConfigureAwait(false);
        await dst.SyncAsync(cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>Outcome of a snapshot restore (<see cref="SnapshotReceiver"/>).</summary>
/// <param name="SequenceNumber">The sequence number the source's checkpoint covers (from the plan).</param>
/// <param name="ReusedFiles">Local files kept because the plan confirmed them.</param>
/// <param name="TransferredFiles">Files written from the stream.</param>
/// <param name="TransferredBytes">Bytes written from the stream.</param>
public sealed record SnapshotRestoreResult(ulong SequenceNumber, int ReusedFiles, int TransferredFiles, long TransferredBytes);

/// <summary>
/// Consumer side of a snapshot transfer: applies the <see cref="SnapshotChunk"/>s produced by
/// <see cref="ReplicationSession.StreamSnapshotAsync"/> to a database directory. On the plan it runs
/// <see cref="ReplicationDelta.PrepareForRestore(IFileSystem, string, IEnumerable{string}, IEnumerable{string})"/> (deleting
/// the WAL directory and every file the plan does not keep, except <c>preserveNames</c>), then writes each file. The
/// sender transfers CURRENT last, so a receiver that dies mid-transfer leaves no CURRENT behind and the next attempt
/// starts over cleanly; <see cref="CompleteAsync"/> also requires the stream's end marker. Ported from the snapshot pull of rocksdb-sharp's ClusterTest / SwitchoverTest hosts.
/// <para>
/// The receiver does not rely on the sender for what makes a restore atomic: CURRENT is written under a temporary name
/// and renamed into place by <see cref="CompleteAsync"/>, after the end marker and after every other file was synced -
/// so no order of files and no cut can leave a directory that opens as a database; and a plan may only keep immutable
/// files (.sst / .blob) that exist here - a plan that keeps CURRENT, a MANIFEST or a WAL, or names a file this node
/// does not hold, is refused before anything is deleted.
/// </para>
/// </summary>
public sealed class SnapshotReceiver : IAsyncDisposable
{
    private readonly IFileSystem _fs;
    private readonly string _dbPath;
    private readonly string[] _preserve;
    private bool _planApplied;
    private bool _complete;
    private ulong _sequence;
    private int _reused;
    private int _files;
    private long _bytes;
    private IWritableFile? _current;
    private string? _currentName;
    private long _currentSize;
    private bool _currentFileReceived;

    private const string CurrentFileName = "CURRENT";
    // Not a name RocksDB knows; a leftover is deleted by the next restore (it is never in a plan's keep list).
    private const string PendingCurrentFileName = "CURRENT.restoring";

    public SnapshotReceiver(IFileSystem fileSystem, string dbPath, IEnumerable<string> preserveNames)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(dbPath);
        _fs = fileSystem;
        _dbPath = dbPath;
        _preserve = [.. preserveNames ?? []];
    }

    /// <summary>Receives a whole stream and completes the restore.</summary>
    public static async ValueTask<SnapshotRestoreResult> ReceiveAsync(IAsyncEnumerable<SnapshotChunk> stream, IFileSystem fileSystem, string dbPath,
        IEnumerable<string> preserveNames, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        await using var receiver = new SnapshotReceiver(fileSystem, dbPath, preserveNames);
        await foreach (SnapshotChunk chunk in stream.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await receiver.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }
        return await receiver.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask WriteAsync(SnapshotChunk chunk, CancellationToken cancellationToken)
    {
        if (chunk.Kind == SnapshotChunkKind.Plan)
        {
            if (_planApplied) throw new InvalidOperationException("snapshot stream sent two plans");
            foreach (string keep in chunk.KeepFiles)
            {
                if (!ReplicationDelta.IsPlainFileName(keep) || !ReplicationDelta.IsImmutableFile(keep))
                    throw new InvalidOperationException("snapshot plan keeps " + keep + ", which is not an immutable table or blob file");
                if (!_fs.FileExists(FileSystemHelpers.Combine(_dbPath, keep)))
                    throw new InvalidOperationException("snapshot plan keeps " + keep + ", which this node does not hold");
            }
            ReplicationDelta.PrepareForRestore(_fs, _dbPath, chunk.KeepFiles, _preserve);
            _reused = chunk.KeepFiles.Count;
            _sequence = chunk.SequenceNumber;
            _planApplied = true;
            return;
        }
        if (!_planApplied) throw new InvalidOperationException("snapshot stream did not start with a delta plan");
        if (_complete) throw new InvalidOperationException("snapshot stream continued after its end marker");
        if (chunk.Kind == SnapshotChunkKind.Complete)
        {
            await CloseCurrentAsync(cancellationToken).ConfigureAwait(false);
            if (chunk.FileSize != _files) throw new InvalidOperationException("snapshot stream announced " + chunk.FileSize + " files, received " + _files);
            _complete = true;
            return;
        }
        if (chunk.Kind != SnapshotChunkKind.FileData) throw new InvalidOperationException("snapshot stream sent a chunk of unknown kind " + (int)chunk.Kind);
        if (!ReplicationDelta.IsPlainFileName(chunk.FileName) || chunk.FileName == PendingCurrentFileName)
            throw new InvalidOperationException("snapshot stream sent an invalid file name: " + chunk.FileName);
        foreach (string preserved in _preserve)
        {
            if (string.Equals(preserved, chunk.FileName, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("snapshot stream sent " + chunk.FileName + ", which is this node's own state");
        }
        if (_currentName != chunk.FileName)
        {
            await CloseCurrentAsync(cancellationToken).ConfigureAwait(false);
            _currentName = chunk.FileName;
            bool isCurrent = chunk.FileName == CurrentFileName;
            if (isCurrent) _currentFileReceived = true;
            _current = _fs.CreateWritable(FileSystemHelpers.Combine(_dbPath, isCurrent ? PendingCurrentFileName : chunk.FileName!), truncate: true);
            _currentSize = chunk.FileSize;
            _files++;
        }
        if (chunk.FileSize != _currentSize || chunk.Offset + chunk.Content.Length > _currentSize)
            throw new InvalidOperationException("snapshot stream chunk of " + chunk.FileName + " at " + chunk.Offset + " (" + chunk.Content.Length +
                " bytes, file size " + chunk.FileSize + ") does not fit the announced size " + _currentSize);
        if (chunk.Offset != _current!.Size)
            throw new InvalidOperationException("snapshot stream chunk of " + chunk.FileName + " at " + chunk.Offset + " does not follow " + _current.Size);
        if (!chunk.Content.IsEmpty)
        {
            await _current.AppendAsync(chunk.Content, cancellationToken).ConfigureAwait(false);
            _bytes += chunk.Content.Length;
        }
    }

    /// <summary>Closes and syncs the last file and the directory. Throws if no plan was received (the source refused).</summary>
    public async ValueTask<SnapshotRestoreResult> CompleteAsync(CancellationToken cancellationToken)
    {
        await CloseCurrentAsync(cancellationToken).ConfigureAwait(false);
        if (!_planApplied) throw new InvalidOperationException("snapshot source sent no data (not primary/leader?)");
        // Without the end marker the transfer was cut off; CURRENT goes last, but a partially written file before it
        // must not be taken for a complete restore either.
        if (!_complete) throw new InvalidOperationException("snapshot stream ended before its end marker");
        if (_currentFileReceived)
        {
            // Everything else is in place and synced; only now does the directory become an openable database.
            _fs.SyncDirectory(_dbPath);
            _fs.RenameFile(FileSystemHelpers.Combine(_dbPath, PendingCurrentFileName), FileSystemHelpers.Combine(_dbPath, CurrentFileName));
        }
        _fs.SyncDirectory(_dbPath);
        return new SnapshotRestoreResult(_sequence, _reused, _files, _bytes);
    }

    private async ValueTask CloseCurrentAsync(CancellationToken cancellationToken)
    {
        IWritableFile? f = _current;
        string? name = _currentName;
        _current = null;
        _currentName = null;
        if (f is null) return;
        try
        {
            // Chunk offsets only prove the chunks were contiguous; a file whose tail never arrived (the next file or the
            // end marker followed a short last chunk) would otherwise be restored truncated.
            if (f.Size != _currentSize)
                throw new InvalidOperationException("snapshot stream sent " + f.Size + " bytes of " + name + ", announced " + _currentSize);
            await f.SyncAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await f.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        IWritableFile? f = _current;
        _current = null;
        if (f is not null) await f.DisposeAsync().ConfigureAwait(false);
    }
}
