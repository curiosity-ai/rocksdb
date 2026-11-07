using RocksDb.Managed.Db;

namespace RocksDb.Managed;

public sealed partial class RocksDatabase
{
    /// <summary>
    /// DBImpl::CancelAllBackgroundWork (the C API's rocksdb_cancel_all_background_work): begins the shutdown of the DB
    /// without closing it. Unless a shutdown already began, memtables holding data that is not in the WAL are flushed
    /// first (skipped with <see cref="DbOptions.AvoidFlushDuringShutdown"/>) and listeners get
    /// <see cref="EventListener.OnDBShutdownBegin"/>; then no new flush or compaction is scheduled and running
    /// compactions are canceled. With <paramref name="wait"/> the call returns once the running background jobs finished.
    /// <para>
    /// The DB stays usable for reads; writes still reach the WAL and memtables but are no longer flushed, so a write
    /// that needs a flush to make room can stall, and Flush / CompactRange fail with ShutdownInProgress or
    /// ObjectDisposed. Call it before closing to stop background work early, e.g. at process shutdown; closing
    /// afterwards does not flush again. Thread-safe; calling it twice is harmless.
    /// </para>
    /// </summary>
    public void CancelAllBackgroundWork(bool wait)
    {
        ValueTask t = _db.CancelAllBackgroundWorkAsync(wait, synchronous: true);
        if (!t.IsCompleted) t.AsTask().GetAwaiter().GetResult();
        else t.GetAwaiter().GetResult();
    }

    /// <inheritdoc cref="CancelAllBackgroundWork"/>
    public ValueTask CancelAllBackgroundWorkAsync(bool wait) => _db.CancelAllBackgroundWorkAsync(wait, synchronous: false);

    /// <summary>
    /// rocksdb::DestroyDB (db/db_impl/db_impl.cc): deletes the database at <paramref name="path"/> -- its table, blob and
    /// WAL files, MANIFEST, CURRENT, IDENTITY, OPTIONS and info LOG files, the archived WALs, the WALs of
    /// <see cref="DbOptions.WalDir"/> and the table / blob files of <see cref="DbOptions.DbPaths"/> -- then removes the
    /// directories if they are empty. Files the DB does not own are kept. With an
    /// <see cref="DbOptions.SstFileManager"/>, table, blob and WAL files are deleted through it (rate-limited trash) and
    /// the call returns once its trash is empty.
    /// <para>
    /// The DB must not be open: the DB LOCK is taken first, and failing to take it (the DB is open in this or another
    /// process) throws <see cref="StatusCode.IOError"/> without deleting anything. Use the same
    /// <see cref="DbOptions.WalDir"/>, <see cref="DbOptions.DbPaths"/> and <see cref="DbOptions.DbLogDir"/> as the DB
    /// was opened with. Not thread-safe with respect to other operations on the same directory.
    /// </para>
    /// </summary>
    /// <exception cref="RocksDbException">IOError if the LOCK cannot be taken or a file could not be deleted.</exception>
    public static void Destroy(string path, DbOptions options)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        DestroyDb.Run(path, options);
    }

    /// <summary>
    /// <see cref="Destroy(string, DbOptions)"/> on the thread pool: deletion is a sequence of blocking file system
    /// calls, and with a rate-limited <see cref="DbOptions.SstFileManager"/> it waits for the trash to drain.
    /// </summary>
    public static ValueTask DestroyAsync(string path, DbOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        return new ValueTask(Task.Run(() => DestroyDb.Run(path, options), cancellationToken));
    }
}
