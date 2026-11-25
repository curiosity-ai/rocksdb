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
public sealed record SnapshotRestoreResult(ulong SequenceNumber, int ReusedFiles, int TransferredFiles, long TransferredBytes)
{
    /// <summary>The description of the log the source sent with the plan (<see cref="SnapshotChunk.LogDescription"/>), or null.</summary>
    public string? LogDescription { get; init; }

    /// <summary>Whether the snapshot was restored next to the old copy and swapped in (<see cref="SnapshotRestoreMode"/>).</summary>
    public bool Swapped { get; init; }
}

/// <summary>How a snapshot restore treats the database its directory holds.</summary>
public enum SnapshotRestoreMode
{
    /// <summary>
    /// Restore-then-swap when the directory holds a database and the volume has room for the announced transfer plus
    /// <see cref="SnapshotRestoreOptions.FreeSpaceMarginBytes"/>; delete-first otherwise - also when the free space
    /// cannot be determined (the file system does not report it) or the source did not announce the size.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Always restore next to the current copy and swap (when there is one), whatever the free space: the node keeps an
    /// openable copy until the new one is complete. Needs room for the transferred files next to the old ones.
    /// </summary>
    AlwaysSwap = 1,

    /// <summary>Always delete the current copy first and restore in place: least disk space, no copy during the restore.</summary>
    AlwaysDeleteFirst = 2,
}

/// <summary>Options of <see cref="SnapshotReceiver.ReceiveAsync(IAsyncEnumerable{SnapshotChunk}, IFileSystem, string, IEnumerable{string}, SnapshotRestoreOptions, CancellationToken)"/>.</summary>
public sealed class SnapshotRestoreOptions
{
    /// <summary>Default of <see cref="FreeSpaceMarginBytes"/>: 64 MiB.</summary>
    public const long DefaultFreeSpaceMarginBytes = 64L << 20;

    /// <summary><see cref="SnapshotReceiver.Mode"/>.</summary>
    public SnapshotRestoreMode Mode { get; init; } = SnapshotRestoreMode.AlwaysDeleteFirst;

    /// <summary><see cref="SnapshotReceiver.FreeSpaceMarginBytes"/>.</summary>
    public long FreeSpaceMarginBytes { get; init; } = DefaultFreeSpaceMarginBytes;

    /// <summary><see cref="SnapshotReceiver.RequireLogDescription"/>.</summary>
    public bool RequireLogDescription { get; init; }

    /// <summary><see cref="SnapshotReceiver.OnLogDescription"/>.</summary>
    public Action<string>? OnLogDescription { get; init; }

    /// <summary><see cref="SnapshotReceiver.SwapGuard"/>.</summary>
    public Action<Action>? SwapGuard { get; init; }

    /// <summary><see cref="SnapshotReceiver.Log"/>.</summary>
    public Action<string>? Log { get; init; }
}

/// <summary>
/// The directory steps of a restore-then-swap (<see cref="SnapshotRestoreMode"/>) and the recovery of one that a crash
/// interrupted. With the database at <c>D</c>, the staged copy is written to <c>D.resync-new</c>; the swap renames
/// <c>D</c> to <c>D.resync-old</c>, <c>D.resync-new</c> to <c>D</c>, moves the preserved entries (the owner's own
/// state) from the old copy into the new one, and the old copy is deleted. What a crash leaves, and what
/// <see cref="Recover"/> makes of it:
/// <list type="bullet">
/// <item>while the staged copy is written, or complete but not swapped: <c>D</c> is the old database, untouched;
/// the staged copy is discarded (whatever asked for the resync asks again).</item>
/// <item>between the two renames: there is no <c>D</c>; the staged copy (it has a CURRENT) becomes <c>D</c>, and the
/// case below applies.</item>
/// <item>after the second rename: <c>D</c> is the new database; preserved entries still in the old copy are moved over,
/// the old copy is deleted, and the description of the new log (<see cref="PendingDescriptionFileName"/> in <c>D</c>)
/// is returned for the owner to apply before it uses its state.</item>
/// </list>
/// The description is applied after the swap because until then the owner's state describes the old database, which is
/// what a crash before the swap leaves.
/// </summary>
public static class SnapshotRestoreSwap
{
    /// <summary>File in the database directory holding the description of the restored log until the owner applied it.</summary>
    public const string PendingDescriptionFileName = "RESTORED_LOG_DESCRIPTION";

    /// <summary>The staging directory of the database directory <paramref name="dbPath"/>.</summary>
    public static string StagePath(string dbPath) => Trim(dbPath) + ".resync-new";

    /// <summary>Where the swap moves the old copy of the database directory <paramref name="dbPath"/>.</summary>
    public static string OldPath(string dbPath) => Trim(dbPath) + ".resync-old";

    private static string Trim(string dbPath)
    {
        string t = dbPath.TrimEnd('/', '\\');
        return t.Length == 0 ? dbPath : t;
    }

    /// <summary>The swap itself: two renames and the move of the preserved entries. The staged copy must be complete.</summary>
    internal static void Swap(IFileSystem fs, string dbPath, IReadOnlyCollection<string> preserveNames)
    {
        string old = OldPath(dbPath);
        fs.RenameFile(dbPath, old);
        fs.RenameFile(StagePath(dbPath), dbPath);
        MovePreserved(fs, old, dbPath, preserveNames);
        SyncParent(fs, dbPath);
    }

    private static void MovePreserved(IFileSystem fs, string from, string to, IReadOnlyCollection<string> preserveNames)
    {
        foreach (string name in preserveNames)
        {
            string src = FileSystemHelpers.Combine(from, name), dst = FileSystemHelpers.Combine(to, name);
            if (!Exists(fs, src) || Exists(fs, dst)) continue;
            fs.RenameFile(src, dst);
        }
        fs.SyncDirectory(to);
    }

    private static bool Exists(IFileSystem fs, string path) => fs.FileExists(path) || FileSystemHelpers.DirectoryExists(fs, path);

    private static void SyncParent(IFileSystem fs, string dbPath)
    {
        string t = Trim(dbPath);
        int slash = t.LastIndexOfAny(['/', '\\']);
        if (slash < 0) return;
        try
        {
            fs.SyncDirectory(slash == 0 ? t[..1] : t[..slash]);
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
        }
    }

    internal static void DeleteStage(IFileSystem fs, string dbPath)
    {
        string stage = StagePath(dbPath);
        if (FileSystemHelpers.DirectoryExists(fs, stage)) FileSystemHelpers.DeleteDirectoryRecursive(fs, stage);
    }

    /// <summary>Deletes the old copy a swap moved aside (best effort; <see cref="Recover"/> removes a leftover).</summary>
    internal static void DeleteOld(IFileSystem fs, string dbPath, Action<string>? log)
    {
        string old = OldPath(dbPath);
        try
        {
            if (FileSystemHelpers.DirectoryExists(fs, old)) FileSystemHelpers.DeleteDirectoryRecursive(fs, old);
            SyncParent(fs, dbPath);
        }
        catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException)
        {
            log?.Invoke("could not remove the old copy " + old + " after the swap (removed at the next start): " + e.Message);
        }
    }

    /// <summary>Removes the description file once the owner applied it.</summary>
    public static void ClearPendingDescription(IFileSystem fileSystem, string dbPath)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        string path = FileSystemHelpers.Combine(dbPath, PendingDescriptionFileName);
        if (!fileSystem.FileExists(path)) return;
        fileSystem.DeleteFile(path);
        fileSystem.SyncDirectory(dbPath);
    }

    /// <summary>
    /// Completes or discards a restore-then-swap that an earlier process did not finish (see the class remarks). Call
    /// it before the database directory is created or opened and before the owner loads the state it keeps in a
    /// preserved entry. Returns the description of the restored log that the owner still has to apply (then call
    /// <see cref="ClearPendingDescription"/>), or null.
    /// </summary>
    public static string? Recover(IFileSystem fileSystem, string dbPath, IReadOnlyCollection<string> preserveNames, Action<string>? log)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(dbPath);
        preserveNames ??= [];
        string old = OldPath(dbPath), stage = StagePath(dbPath);
        const string Current = "CURRENT";
        if (FileSystemHelpers.DirectoryExists(fileSystem, old))
        {
            bool dbExists = FileSystemHelpers.DirectoryExists(fileSystem, dbPath);
            if (!dbExists)
            {
                // Between the two renames.
                if (FileSystemHelpers.DirectoryExists(fileSystem, stage) && fileSystem.FileExists(FileSystemHelpers.Combine(stage, Current)))
                {
                    fileSystem.RenameFile(stage, dbPath);
                    log?.Invoke("completed an interrupted snapshot swap: the restored copy is now " + dbPath);
                }
                else
                {
                    fileSystem.RenameFile(old, dbPath);
                    log?.Invoke("undid an interrupted snapshot swap: " + dbPath + " is the previous copy again");
                }
            }
            if (FileSystemHelpers.DirectoryExists(fileSystem, old))
            {
                if (fileSystem.FileExists(FileSystemHelpers.Combine(dbPath, Current)))
                {
                    // The new copy is in place: bring the owner's state over, drop the old copy.
                    MovePreserved(fileSystem, old, dbPath, preserveNames);
                    DeleteOld(fileSystem, dbPath, log);
                }
                else
                {
                    // Not a state a swap leaves (something else created the directory): keep both, say so.
                    log?.Invoke(dbPath + " holds no database but " + old + " exists; leaving both in place");
                }
            }
            SyncParent(fileSystem, dbPath);
        }
        if (FileSystemHelpers.DirectoryExists(fileSystem, stage))
        {
            // Written but never swapped in (or a leftover): the database directory is what counts.
            FileSystemHelpers.DeleteDirectoryRecursive(fileSystem, stage);
            log?.Invoke("removed the staged snapshot copy " + stage + " of an interrupted restore");
        }
        return FileSystemHelpers.DirectoryExists(fileSystem, dbPath)
            ? FileSystemHelpers.TryReadAllText(fileSystem, FileSystemHelpers.Combine(dbPath, PendingDescriptionFileName))
            : null;
    }
}

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
    private string? _logDescription;
    // Where the files are written: the database directory (delete-first) or its staging sibling (restore-then-swap).
    private string _target;
    private bool _swap;

    /// <summary>How the restore treats the database the directory holds now (default: <see cref="SnapshotRestoreMode.AlwaysDeleteFirst"/>).</summary>
    public SnapshotRestoreMode Mode { get; init; } = SnapshotRestoreMode.AlwaysDeleteFirst;

    /// <summary>
    /// <see cref="SnapshotRestoreMode.Auto"/>: free space that must remain beyond the bytes the plan announces for the
    /// restore to be staged next to the current copy (default 64 MiB).
    /// </summary>
    public long FreeSpaceMarginBytes { get; init; } = SnapshotRestoreOptions.DefaultFreeSpaceMarginBytes;

    /// <summary>Refuse a plan without <see cref="SnapshotChunk.LogDescription"/> (before anything is deleted).</summary>
    public bool RequireLogDescription { get; init; }

    /// <summary>
    /// Restore-then-swap: runs the swap (the directory renames, the move of the preserved entries and
    /// <see cref="OnLogDescription"/>) - for an owner whose own state lives in a preserved entry, to keep that state
    /// from being written while its directory moves. Null: the swap runs as is.
    /// </summary>
    public Action<Action>? SwapGuard { get; init; }

    /// <summary>Receives one line about the mode chosen for the restore, and why.</summary>
    public Action<string>? Log { get; init; }

    /// <summary>Whether this restore was staged next to the old copy and swapped in (known once the plan was received).</summary>
    public bool UsedSwap => _swap;

    /// <summary>
    /// Called by <see cref="CompleteAsync"/> with the plan's <see cref="SnapshotChunk.LogDescription"/> (when the source
    /// sent one) after every file is in place and synced and before CURRENT is renamed into place: what it persists is
    /// durable before the directory opens as a database. A crash in between leaves the description without a database,
    /// and the restore starts over. If it throws, the restore fails and no CURRENT appears.
    /// </summary>
    public Action<string>? OnLogDescription { get; init; }

    private const string CurrentFileName = "CURRENT";
    // Not a name RocksDB knows; a leftover is deleted by the next restore (it is never in a plan's keep list).
    private const string PendingCurrentFileName = "CURRENT.restoring";

    public SnapshotReceiver(IFileSystem fileSystem, string dbPath, IEnumerable<string> preserveNames)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(dbPath);
        _fs = fileSystem;
        _dbPath = dbPath;
        _target = dbPath;
        _preserve = [.. preserveNames ?? []];
    }

    /// <summary>Receives a whole stream and completes the restore with <paramref name="options"/>.</summary>
    public static async ValueTask<SnapshotRestoreResult> ReceiveAsync(IAsyncEnumerable<SnapshotChunk> stream, IFileSystem fileSystem, string dbPath,
        IEnumerable<string> preserveNames, SnapshotRestoreOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(options);
        await using var receiver = new SnapshotReceiver(fileSystem, dbPath, preserveNames)
        {
            OnLogDescription = options.OnLogDescription,
            Mode = options.Mode,
            FreeSpaceMarginBytes = options.FreeSpaceMarginBytes,
            RequireLogDescription = options.RequireLogDescription,
            SwapGuard = options.SwapGuard,
            Log = options.Log,
        };
        await foreach (SnapshotChunk chunk in stream.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            await receiver.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
        }
        return await receiver.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Receives a whole stream and completes the restore.</summary>
    public static ValueTask<SnapshotRestoreResult> ReceiveAsync(IAsyncEnumerable<SnapshotChunk> stream, IFileSystem fileSystem, string dbPath,
        IEnumerable<string> preserveNames, CancellationToken cancellationToken) =>
        ReceiveAsync(stream, fileSystem, dbPath, preserveNames, (Action<string>?)null, cancellationToken);

    /// <summary>Receives a whole stream and completes the restore; <paramref name="onLogDescription"/> is <see cref="OnLogDescription"/>.</summary>
    public static async ValueTask<SnapshotRestoreResult> ReceiveAsync(IAsyncEnumerable<SnapshotChunk> stream, IFileSystem fileSystem, string dbPath,
        IEnumerable<string> preserveNames, Action<string>? onLogDescription, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        await using var receiver = new SnapshotReceiver(fileSystem, dbPath, preserveNames) { OnLogDescription = onLogDescription };
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
            if (RequireLogDescription && chunk.LogDescription is null)
                throw new InvalidOperationException("snapshot plan carries no description of the log (term / epoch / lineage map)");
            // A swap an earlier process did not finish (the hosts do this at start, before they load their state).
            SnapshotRestoreSwap.Recover(_fs, _dbPath, _preserve, Log);
            _swap = DecideSwap(chunk.TransferBytes, out string why);
            if (_swap)
            {
                try
                {
                    PrepareStage(chunk.KeepFiles);
                }
                catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException or NotSupportedException)
                {
                    // E.g. a file system without hard links: the kept files cannot be shared with the staged copy.
                    _swap = false;
                    why = "the staging directory could not be prepared (" + e.Message + ")";
                    SnapshotRestoreSwap.DeleteStage(_fs, _dbPath);
                }
            }
            Log?.Invoke("snapshot restore: " + (_swap ? "restore-then-swap" : "delete-first") + " (" + why + ")");
            if (_swap)
            {
                _target = SnapshotRestoreSwap.StagePath(_dbPath);
            }
            else
            {
                _target = _dbPath;
                ReplicationDelta.PrepareForRestore(_fs, _dbPath, chunk.KeepFiles, _preserve);
            }
            _reused = chunk.KeepFiles.Count;
            _sequence = chunk.SequenceNumber;
            _logDescription = chunk.LogDescription;
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
            _current = _fs.CreateWritable(FileSystemHelpers.Combine(_target, isCurrent ? PendingCurrentFileName : chunk.FileName!), truncate: true);
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
        // A checkpoint always has a CURRENT, and a plan cannot keep the local one: a stream without it is not a snapshot.
        // Accepting it left a directory that the node then created an empty database in (at sequence number 0).
        if (!_currentFileReceived) throw new InvalidOperationException("snapshot stream ended without CURRENT");
        if (_swap)
        {
            CompleteSwap();
            return new SnapshotRestoreResult(_sequence, _reused, _files, _bytes) { LogDescription = _logDescription, Swapped = true };
        }
        // Everything else is in place and synced; only now does the directory become an openable database.
        _fs.SyncDirectory(_dbPath);
        if (_logDescription is not null) OnLogDescription?.Invoke(_logDescription);
        _fs.RenameFile(FileSystemHelpers.Combine(_dbPath, PendingCurrentFileName), FileSystemHelpers.Combine(_dbPath, CurrentFileName));
        _fs.SyncDirectory(_dbPath);
        return new SnapshotRestoreResult(_sequence, _reused, _files, _bytes) { LogDescription = _logDescription };
    }

    /// <summary>
    /// Whether this restore is staged next to the current copy. Only when there is a copy worth keeping (the directory
    /// opens as a database), and with <see cref="SnapshotRestoreMode.Auto"/> only when the volume has room for the
    /// announced bytes plus the margin; unknown free space or an unknown size means delete-first.
    /// </summary>
    private bool DecideSwap(long transferBytes, out string why)
    {
        if (Mode == SnapshotRestoreMode.AlwaysDeleteFirst)
        {
            why = "forced by the restore mode";
            return false;
        }
        if (!_fs.FileExists(FileSystemHelpers.Combine(_dbPath, CurrentFileName)))
        {
            why = "there is no database here to keep";
            return false;
        }
        if (Mode == SnapshotRestoreMode.AlwaysSwap)
        {
            why = "forced by the restore mode";
            return true;
        }
        if (transferBytes < 0)
        {
            why = "the source did not announce the size of the transfer";
            return false;
        }
        long free;
        try
        {
            free = _fs.GetFreeSpace(_dbPath);
        }
        catch (Exception e) when (e is IOException or RocksDbException or NotSupportedException or NotImplementedException or UnauthorizedAccessException)
        {
            why = "the free disk space is unknown (" + e.Message + ")";
            return false;
        }
        long margin = Math.Max(0, FreeSpaceMarginBytes);
        long needed = transferBytes > long.MaxValue - margin ? long.MaxValue : transferBytes + margin;
        if (free < needed)
        {
            why = "not enough free disk space for a second copy: " + free + " bytes free, " + transferBytes + " to transfer plus a margin of " + margin;
            return false;
        }
        why = free + " bytes free for " + transferBytes + " to transfer plus a margin of " + margin;
        return true;
    }

    /// <summary>Creates the (empty) staging directory and hard-links the kept files of the current copy into it.</summary>
    private void PrepareStage(IReadOnlyList<string> keepFiles)
    {
        string stage = SnapshotRestoreSwap.StagePath(_dbPath);
        SnapshotRestoreSwap.DeleteStage(_fs, _dbPath);
        _fs.CreateDirectoryIfMissing(stage);
        foreach (string keep in keepFiles) _fs.LinkFile(FileSystemHelpers.Combine(_dbPath, keep), FileSystemHelpers.Combine(stage, keep));
    }

    /// <summary>
    /// Restore-then-swap, once every file of the stream is in the staging directory and synced. The description of the
    /// log is first written into the staging directory, so that it moves with the files; then CURRENT appears there
    /// (the staged copy is complete); then the swap: the current copy is renamed aside, the staged one takes its name,
    /// the preserved entries move over and the description is applied - under <see cref="SwapGuard"/>. The old copy is
    /// deleted last. See <see cref="SnapshotRestoreSwap"/> for what a crash at each step leaves.
    /// </summary>
    private void CompleteSwap()
    {
        string stage = SnapshotRestoreSwap.StagePath(_dbPath);
        _fs.SyncDirectory(stage);
        if (_logDescription is not null)
            FileSystemHelpers.AtomicWrite(_fs, FileSystemHelpers.Combine(stage, SnapshotRestoreSwap.PendingDescriptionFileName), _logDescription);
        _fs.RenameFile(FileSystemHelpers.Combine(stage, PendingCurrentFileName), FileSystemHelpers.Combine(stage, CurrentFileName));
        _fs.SyncDirectory(stage);
        void Swap()
        {
            SnapshotRestoreSwap.Swap(_fs, _dbPath, _preserve);
            if (_logDescription is not null)
            {
                OnLogDescription?.Invoke(_logDescription);
                SnapshotRestoreSwap.ClearPendingDescription(_fs, _dbPath);
            }
        }
        if (SwapGuard is { } guard) guard(Swap);
        else Swap();
        SnapshotRestoreSwap.DeleteOld(_fs, _dbPath, Log);
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
