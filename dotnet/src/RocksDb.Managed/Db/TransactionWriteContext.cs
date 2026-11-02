namespace RocksDb.Managed.Db;

/// <summary>
/// Port of rocksdb::WriteCallback (db/write_callback.h): checked by the write-group leader, after PreprocessWrite and
/// before sequence allocation, while no other write can be applied. A failing callback excludes its writer from the
/// group (nothing reaches the WAL or the memtables) and fails that write with the returned error.
/// </summary>
internal interface IWriteCallback
{
    /// <summary>Returns null to proceed with the write, or the error to fail it with (e.g. Busy / TryAgain).</summary>
    RocksDbException? Check(DbImpl db);

    /// <summary>False if the writer must not be grouped behind another leader (WriteCallback::AllowWriteBatching).</summary>
    bool AllowWriteBatching { get; }
}

/// <summary>
/// Per-write extras used by transactions, mirroring the extra arguments of DBImpl::WriteImpl
/// (db/db_impl/db_impl_write.cc): <c>callback</c>, <c>disable_memtable</c>, <c>log_ref</c>, <c>log_used</c>, the
/// MarkLogCallback pre-release callback of WriteCommittedTxn::PrepareInternal, and the WAL termination point of
/// WriteCommittedTxn::CommitInternal (modelled as a separate memtable batch). Reusable; owned by one transaction.
/// </summary>
internal sealed class TransactionWriteContext
{
    /// <summary>Conflict check run in the write group (optimistic transactions, kValidateSerial).</summary>
    public IWriteCallback? Callback;

    /// <summary>
    /// When set, the WAL record is the writer's batch and this batch is what is inserted into the memtables (the
    /// commit marker goes to the WAL while the prepared batch goes to the memtables). Must use the same sequence
    /// numbering as if it had been written itself.
    /// </summary>
    public WriteBatch? MemtableBatch;

    /// <summary>WAL only: no memtable insert and no sequence numbers consumed (2PC prepare).</summary>
    public bool DisableMemtable;

    /// <summary>The WAL holding the prepared section of the data being inserted (memtables keep that WAL alive).</summary>
    public ulong LogRef;

    /// <summary>Records the WAL that received the record as containing an outstanding prepared section.</summary>
    public bool MarkLogContainingPrepSection;

    /// <summary>
    /// batch_cnt of DBImpl::WriteImpl: the number of sub-batches (sequence numbers) the write consumes in a
    /// sequence-per-batch DB (WritePrepared / WriteUnprepared). 0 = not given (the write is not from a transaction).
    /// </summary>
    public int BatchCount;

    /// <summary>Run after the WAL write and before the memtable insert / sequence publication (pre_release_callback).</summary>
    public IPreReleaseCallback? PreRelease;

    /// <summary>Output: the WAL number the record was written to (log_used).</summary>
    public ulong LogUsed;

    /// <summary>Output: the first sequence number assigned to the write (seq_used).</summary>
    public ulong SequenceUsed;

    public void Reset()
    {
        Callback = null;
        MemtableBatch = null;
        DisableMemtable = false;
        LogRef = 0;
        MarkLogContainingPrepSection = false;
        LogUsed = 0;
        SequenceUsed = 0;
        BatchCount = 0;
        PreRelease = null;
    }
}
