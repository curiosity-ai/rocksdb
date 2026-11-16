namespace RocksDb.Managed;

/// <summary>
/// Callbacks invoked while a write passed to <see cref="RocksDatabase.WriteWithCallback(WriteOptions?, Db.WriteBatch, UserWriteCallback)"/>
/// progresses, e.g. to synchronize concurrent writers. Port of <c>rocksdb::UserWriteCallback</c>
/// (include/rocksdb/user_write_callback.h).
/// <para>
/// The callbacks run in the write's critical path (possibly on the thread of another writer that leads the write
/// group), so they should be fast, should not throw and must not write to the DB.
/// </para>
/// <para>
/// A callback that throws fails safely (upstream: undefined behaviour): the exception is caught, the write still goes
/// through its write group (the other writes of the group get the outcome they would have had), and then the caller of
/// <c>WriteWithCallback</c> / <c>WriteWithCallbackAsync</c> gets that exception, unchanged, and the database gets a
/// hard <see cref="RocksDatabase.BackgroundError"/> that fails further writes until <see cref="RocksDatabase.Resume"/>.
/// The outcome of the throwing write is indeterminate, like a write that failed after its WAL record was written: it
/// may have been committed (and be readable, and recovered from the WAL by a reopen), or not.
/// </para>
/// </summary>
public abstract class UserWriteCallback
{
    /// <summary>Called once the write has been enqueued in the write queue (WriteThread::JoinBatchGroup).</summary>
    public abstract void OnWriteEnqueued();

    /// <summary>
    /// Called once the write's WAL record has been written (and synced when requested). Not called when the write
    /// disables the WAL, when its WAL write fails, or when the write is rejected before reaching the WAL.
    /// </summary>
    public abstract void OnWalWriteFinish();
}
