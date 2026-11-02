using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;

namespace RocksDb.Managed;

/// <summary>
/// Operational / replication APIs of <see cref="RocksDatabase"/>: WAL tailing (GetUpdatesSince), file deletion
/// control, live file listing, checkpoints, external file ingestion, read-only and secondary instances.
/// </summary>
public sealed partial class RocksDatabase
{
    // =====================================================================================================
    // Read-only / secondary open
    // =====================================================================================================

    /// <summary>
    /// Opens the DB read-only (DB::OpenForReadOnly, db/db_impl/db_impl_readonly.cc): no LOCK is taken and nothing is
    /// written; WALs are replayed into memtables; writes, flushes, ingestion and column family changes throw
    /// NotSupported. A subset of the column families may be opened.
    /// </summary>
    public static RocksDatabase OpenReadOnly(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, bool errorIfWalFileExists)
    {
        ValueTask<DbImpl> t = DbImpl.OpenReadOnlyAsync(path, options, columnFamilies, errorIfWalFileExists, null, synchronous: true, CancellationToken.None);
        return new RocksDatabase(t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult());
    }

    public static RocksDatabase OpenReadOnly(string path, DbOptions options) =>
        OpenReadOnly(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], false);

    public static async ValueTask<RocksDatabase> OpenReadOnlyAsync(string path, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies,
        bool errorIfWalFileExists, CancellationToken cancellationToken = default) =>
        new(await DbImpl.OpenReadOnlyAsync(path, options, columnFamilies, errorIfWalFileExists, null, synchronous: false, cancellationToken).ConfigureAwait(false));

    public static ValueTask<RocksDatabase> OpenReadOnlyAsync(string path, DbOptions options, CancellationToken cancellationToken = default) =>
        OpenReadOnlyAsync(path, options, [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], false,
            cancellationToken);

    /// <summary>
    /// Opens a secondary instance following the primary DB at <paramref name="path"/> (DB::OpenAsSecondary,
    /// db/db_impl/db_impl_secondary.cc). <paramref name="secondaryPath"/> is this instance's private directory.
    /// Call <see cref="TryCatchUpWithPrimaryAsync"/> to see the primary's newer writes. Read-only otherwise.
    /// </summary>
    public static async ValueTask<RocksDatabase> OpenAsSecondaryAsync(string path, string secondaryPath, DbOptions options,
        IReadOnlyList<ColumnFamilyDescriptor> columnFamilies, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(secondaryPath);
        return new(await DbImpl.OpenReadOnlyAsync(path, options, columnFamilies, false, secondaryPath, synchronous: false, cancellationToken)
            .ConfigureAwait(false));
    }

    public static ValueTask<RocksDatabase> OpenAsSecondaryAsync(string path, string secondaryPath, DbOptions options,
        CancellationToken cancellationToken = default) =>
        OpenAsSecondaryAsync(path, secondaryPath, options,
            [new ColumnFamilyDescriptor(ColumnFamilyDescriptor.DefaultColumnFamilyName, new ColumnFamilyOptions())], cancellationToken);

    public static RocksDatabase OpenAsSecondary(string path, string secondaryPath, DbOptions options, IReadOnlyList<ColumnFamilyDescriptor> columnFamilies)
    {
        ArgumentException.ThrowIfNullOrEmpty(secondaryPath);
        ValueTask<DbImpl> t = DbImpl.OpenReadOnlyAsync(path, options, columnFamilies, false, secondaryPath, synchronous: true, CancellationToken.None);
        return new RocksDatabase(t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult());
    }

    /// <summary>True for instances opened with <see cref="OpenReadOnly(string, DbOptions)"/> or as secondary.</summary>
    public bool IsReadOnly => _db.IsReadOnly;

    /// <summary>True for instances opened with <see cref="OpenAsSecondaryAsync(string, string, DbOptions, CancellationToken)"/>.</summary>
    public bool IsSecondary => _db.IsSecondary;

    /// <summary>
    /// Makes the secondary see the primary's latest state (DB::TryCatchUpWithPrimary): new table files from the
    /// MANIFEST and writes appended to the WALs since the last catch-up.
    /// </summary>
    public ValueTask TryCatchUpWithPrimaryAsync(CancellationToken cancellationToken = default) =>
        _db.TryCatchUpWithPrimaryAsync(synchronous: false, cancellationToken);

    public void TryCatchUpWithPrimary()
    {
        ValueTask t = _db.TryCatchUpWithPrimaryAsync(synchronous: true, CancellationToken.None);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    // =====================================================================================================
    // WAL tailing
    // =====================================================================================================

    /// <summary>
    /// Iterates the write batches in the WAL starting with the batch that contains <paramref name="sequenceNumber"/>
    /// (DB::GetUpdatesSince). Throws NotFound if the sequence number was not written yet. WALs that were deleted
    /// (flushed and not archived, see <see cref="DbOptions.WalTtlSeconds"/> / <see cref="DbOptions.WalSizeLimitMb"/>)
    /// cannot be read: the iterator then starts at the oldest available batch, so callers must compare
    /// <see cref="WalBatch.Sequence"/> with what they asked for. Disable file deletions to keep WALs alive.
    /// <code>
    /// using var it = db.GetUpdatesSince(seq);
    /// await foreach (WalBatch b in it) replica.Write(b.Span);   // batches available now
    /// </code>
    /// </summary>
    public TransactionLogIterator GetUpdatesSince(ulong sequenceNumber) => _db.GetUpdatesSince(sequenceNumber, verifyChecksums: true);

    public TransactionLogIterator GetUpdatesSince(ulong sequenceNumber, bool verifyChecksums) => _db.GetUpdatesSince(sequenceNumber, verifyChecksums);

    /// <summary>
    /// Tails the WAL forever from <paramref name="sequenceNumber"/>: yields every batch whose last sequence number is
    /// at least <paramref name="sequenceNumber"/>, awaiting new writes when caught up (no polling), following WAL
    /// rotations, until <paramref name="cancellationToken"/> is cancelled or the DB is closed. Each
    /// <see cref="WalBatch"/> is valid until the enumeration moves on. Throws if the WAL has a gap (e.g. the
    /// requested WAL was already deleted) or is corrupted.
    /// </summary>
    public async IAsyncEnumerable<WalBatch> TailUpdatesAsync(ulong sequenceNumber, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (!await _db.WaitForSequenceAsync(sequenceNumber, cancellationToken).ConfigureAwait(false)) yield break;
        using TransactionLogIterator it = _db.GetUpdatesSince(sequenceNumber, verifyChecksums: true);
        ulong expected = sequenceNumber;
        bool first = true;
        while (true)
        {
            if (!it.Valid)
            {
                if (!await it.MoveNextTailingAsync(cancellationToken).ConfigureAwait(false))
                {
                    it.ThrowIfError();
                    yield break;
                }
            }
            WalBatch b = it.Current;
            if (first)
            {
                if (b.Sequence > expected)
                {
                    throw new RocksDbException(StatusCode.NotFound,
                        "NotFound: WAL data for sequence " + expected + " is no longer available (oldest batch starts at " + b.Sequence + ")");
                }
                first = false;
            }
            yield return b;
            it.Next();
        }
    }

    /// <summary>
    /// Completes when the DB has published <paramref name="sequenceNumber"/> (GetLatestSequenceNumber() &gt;= it);
    /// returns false if the DB was closed. Signalled by the write path, no polling.
    /// </summary>
    public ValueTask<bool> WaitForSequenceAsync(ulong sequenceNumber, CancellationToken cancellationToken = default) =>
        _db.WaitForSequenceAsync(sequenceNumber, cancellationToken);

    /// <summary>The WAL files (live and archived) sorted by number, with their first sequence numbers (DB::GetSortedWalFiles).</summary>
    public IReadOnlyList<WalFileInfo> GetSortedWalFiles()
    {
        _db.ThrowIfDisposed();
        return _db.GetSortedWalFiles(includeArchived: true);
    }

    /// <summary>
    /// Deletes the archived WAL <paramref name="logNumber"/> (DB::DeleteFile("/archive/NNNNNN.log")). Lets a
    /// replication source retire archived WALs once every replica has consumed them, instead of relying on
    /// <see cref="DbOptions.WalTtlSeconds"/> alone. Throws NotSupported for a live WAL and NotFound if the file is gone.
    /// </summary>
    public void DeleteArchivedWalFile(ulong logNumber) => _db.DeleteArchivedWalFile(logNumber);

    /// <summary>The file system the DB was opened with (<see cref="DbOptions.FileSystem"/>); checkpoints are written through it.</summary>
    public Env.IFileSystem FileSystem => _db.FileSystem;

    /// <summary>The WAL currently being written (DB::GetCurrentWalFile).</summary>
    public WalFileInfo GetCurrentWalFile() => _db.GetCurrentWalFile();

    /// <summary>Writes buffered WAL data to the OS (manual_wal_flush) and, with <paramref name="sync"/>, fsyncs it (DB::FlushWAL).</summary>
    public void FlushWal(bool sync) => _db.FlushWalAsync(sync, synchronous: true, CancellationToken.None).GetAwaiter().GetResult();

    public ValueTask FlushWalAsync(bool sync, CancellationToken cancellationToken = default) => _db.FlushWalAsync(sync, synchronous: false, cancellationToken);

    /// <summary>fsyncs the WAL (DB::SyncWAL).</summary>
    public void SyncWal() => FlushWal(sync: true);

    public ValueTask SyncWalAsync(CancellationToken cancellationToken = default) => FlushWalAsync(sync: true, cancellationToken);

    // =====================================================================================================
    // File deletions / live files
    // =====================================================================================================

    /// <summary>
    /// Stops deleting obsolete files (SSTs, WALs, MANIFESTs) until a matching <see cref="EnableFileDeletions()"/>
    /// (DB::DisableFileDeletions; calls nest). Returns the new nesting count.
    /// </summary>
    public int DisableFileDeletions() => _db.DisableFileDeletions();

    /// <summary>Undoes one <see cref="DisableFileDeletions"/>; deletes pending obsolete files when the count reaches 0.</summary>
    public int EnableFileDeletions() => _db.EnableFileDeletions(force: false);

    /// <summary>With <paramref name="force"/>, re-enables deletions regardless of the nesting count (legacy DB::EnableFileDeletions(true)).</summary>
    public int EnableFileDeletions(bool force) => _db.EnableFileDeletions(force);

    /// <summary>True unless file deletions are disabled.</summary>
    public bool IsFileDeletionsEnabled => _db.IsFileDeletionsEnabled;

    /// <summary>
    /// The live files (DB::GetLiveFiles): table files, CURRENT, the MANIFEST and the OPTIONS file as names relative to
    /// the DB directory starting with '/', plus the MANIFEST size matching the listed tables. With
    /// <paramref name="flushMemtable"/>, memtables are flushed first.
    /// </summary>
    public async ValueTask<(List<string> Files, ulong ManifestFileSize)> GetLiveFilesAsync(bool flushMemtable, CancellationToken cancellationToken = default) =>
        await _db.GetLiveFilesAsync(flushMemtable, synchronous: false, cancellationToken).ConfigureAwait(false);

    public List<string> GetLiveFiles(bool flushMemtable, out ulong manifestFileSize)
    {
        ValueTask<(List<string> Files, ulong ManifestFileSize)> t = _db.GetLiveFilesAsync(flushMemtable, synchronous: true, CancellationToken.None);
        (List<string> files, ulong size) = t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult();
        manifestFileSize = size;
        return files;
    }

    /// <summary>Names of the live table files (like rocksdb-sharp's GetLiveFileNames: "/000012.sst").</summary>
    public List<string> GetLiveFileNames()
    {
        List<LiveFileMetadata> meta = GetLiveFilesMetadata();
        var names = new List<string>(meta.Count);
        foreach (LiveFileMetadata m in meta) names.Add(m.Name);
        return names;
    }

    // =====================================================================================================
    // Checkpoint
    // =====================================================================================================

    /// <summary>
    /// Creates an openable snapshot of the DB in <paramref name="checkpointDir"/> (which must not exist) using hard
    /// links where possible (Checkpoint::CreateCheckpoint). Memtables are flushed first when the live WALs total at
    /// least <paramref name="logSizeForFlush"/> bytes (0 = always flush, ulong.MaxValue = never: WALs are copied).
    /// Returns the latest sequence number included in the checkpoint.
    /// </summary>
    public ValueTask<ulong> CreateCheckpointAsync(string checkpointDir, ulong logSizeForFlush, CancellationToken cancellationToken = default) =>
        _db.CreateCheckpointAsync(checkpointDir, logSizeForFlush, synchronous: false, cancellationToken);

    public ValueTask<ulong> CreateCheckpointAsync(string checkpointDir, CancellationToken cancellationToken = default) =>
        _db.CreateCheckpointAsync(checkpointDir, 0, synchronous: false, cancellationToken);

    public ulong CreateCheckpoint(string checkpointDir, ulong logSizeForFlush)
    {
        ValueTask<ulong> t = _db.CreateCheckpointAsync(checkpointDir, logSizeForFlush, synchronous: true, CancellationToken.None);
        return t.IsCompleted ? t.Result : t.AsTask().GetAwaiter().GetResult();
    }

    public ulong CreateCheckpoint(string checkpointDir) => CreateCheckpoint(checkpointDir, 0);

    // =====================================================================================================
    // External files
    // =====================================================================================================

    /// <summary>
    /// Ingests SST files written by <see cref="SstFileWriter"/> (or native SstFileWriter) into the default column
    /// family (DB::IngestExternalFile). Files must not overlap each other.
    /// </summary>
    public ValueTask IngestExternalFilesAsync(IReadOnlyList<string> files, IngestExternalFileOptions options, CancellationToken cancellationToken = default) =>
        _db.IngestExternalFilesAsync(_default.Data, files, options, synchronous: false, cancellationToken);

    public ValueTask IngestExternalFilesAsync(ColumnFamily columnFamily, IReadOnlyList<string> files, IngestExternalFileOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return _db.IngestExternalFilesAsync(columnFamily.Data, files, options, synchronous: false, cancellationToken);
    }

    public void IngestExternalFiles(IReadOnlyList<string> files, IngestExternalFileOptions options) =>
        IngestExternalFiles(_default, files, options);

    public void IngestExternalFiles(ColumnFamily columnFamily, IReadOnlyList<string> files, IngestExternalFileOptions options)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        ValueTask t = _db.IngestExternalFilesAsync(columnFamily.Data, files, options, synchronous: true, CancellationToken.None);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    /// <summary>A writer for SST files to ingest into <paramref name="columnFamily"/> (uses its options and file system).</summary>
    public SstFileWriter CreateSstFileWriter(ColumnFamily columnFamily)
    {
        ArgumentNullException.ThrowIfNull(columnFamily);
        return new SstFileWriter(columnFamily.Options, _db.FileSystem, columnFamily);
    }
}
