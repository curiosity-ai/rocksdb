namespace RocksDb.Managed;

/// <summary>
/// Callbacks invoked while a write passed to <see cref="RocksDatabase.WriteWithCallback(WriteOptions?, Db.WriteBatch, UserWriteCallback)"/>
/// progresses, e.g. to synchronize concurrent writers. Port of <c>rocksdb::UserWriteCallback</c>
/// (include/rocksdb/user_write_callback.h).
/// <para>
/// The callbacks run in the write's critical path (possibly on the thread of another writer that leads the write
/// group), so they should be fast and must not throw or write to the DB.
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
