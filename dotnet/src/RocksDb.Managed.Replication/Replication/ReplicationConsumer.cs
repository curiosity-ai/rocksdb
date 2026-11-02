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
    private readonly RocksDatabase _db;

    public ReplicationConsumer(RocksDatabase db)
    {
        ArgumentNullException.ThrowIfNull(db);
        _db = db;
    }

    public RocksDatabase Database => _db;

    /// <summary>The sequence number the next applied batch must start with (latest + 1).</summary>
    public ulong ExpectedNextSequence => _db.GetLatestSequenceNumber() + 1;

    /// <summary>Applies one serialized write batch - a WAL batch or a merged chunk - as one write.</summary>
    public async ValueTask IngestChunkAsync(ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken)
    {
        WriteBatch batch = WriteBatch.Rent();
        try
        {
            batch.SetContents(chunk.Span);
            await _db.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
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
        _db.Write(batch);
    }

    public ValueTask IngestBatchAsync(WalBatch batch, CancellationToken cancellationToken) => IngestChunkAsync(batch.Data, cancellationToken);

    /// <summary>
    /// Applies <paramref name="chunk"/> only if it starts exactly at <see cref="ExpectedNextSequence"/>; returns false
    /// (and writes nothing) otherwise. A gap means the source no longer has the WAL the replica needs; an overlap means
    /// something else wrote to the replica. Either way the replica diverged and must be rebuilt from a checkpoint. The
    /// check assumes this consumer is the replica's only writer.
    /// </summary>
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
        if (!ReplicationDelta.IsPlainFileName(chunk.FileName)) throw new InvalidOperationException("snapshot stream sent an invalid file name: " + chunk.FileName);
        if (_currentName != chunk.FileName)
        {
            await CloseCurrentAsync(cancellationToken).ConfigureAwait(false);
            _currentName = chunk.FileName;
            _current = _fs.CreateWritable(FileSystemHelpers.Combine(_dbPath, chunk.FileName!), truncate: true);
            _files++;
        }
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
        _fs.SyncDirectory(_dbPath);
        return new SnapshotRestoreResult(_sequence, _reused, _files, _bytes);
    }

    private async ValueTask CloseCurrentAsync(CancellationToken cancellationToken)
    {
        IWritableFile? f = _current;
        _current = null;
        _currentName = null;
        if (f is null) return;
        try
        {
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
