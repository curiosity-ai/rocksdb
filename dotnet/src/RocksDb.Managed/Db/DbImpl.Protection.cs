namespace RocksDb.Managed.Db;

/// <summary>
/// WriteBatch per key-value protection on the write path (WriteOptions.protection_bytes_per_key). Port of the
/// protection parts of DBImpl::Write / WriteImpl (UpdateProtectionInfo, the "must be zero or eight" check) and
/// DBImpl::WriteToWAL (merged_batch.VerifyChecksum) in db/db_impl/db_impl_write.cc. The memtable insert side
/// (ProtectionInfoKVOC -&gt; KVOS handed to MemTable::Add, which verifies the encoded entry) is in MemTableInserter.
/// </summary>
internal sealed partial class DbImpl
{
    /// <summary>Set once a protected batch was written: the WAL write then verifies the batches it logs.</summary>
    private int _protectedWrites;

    /// <summary>
    /// DBImpl::Write's UpdateProtectionInfo and WriteImpl's protection_bytes_per_key validation: computes the protection
    /// of <paramref name="batch"/> when the options ask for it (an already protected batch keeps its protection).
    /// </summary>
    private void PrepareProtectedWrite(WriteOptions? options, WriteBatch batch)
    {
        ulong bytes = options?.ProtectionBytesPerKey ?? 0;
        if (bytes is not (0 or 8)) ThrowHelper.InvalidArgument("`WriteOptions::protection_bytes_per_key` must be zero or eight");
        if (bytes != 0) batch.UpdateProtectionInfo((int)bytes);
        if (batch.ProtectionBytesPerKey != 0 && Volatile.Read(ref _protectedWrites) == 0) Interlocked.Exchange(ref _protectedWrites, 1);
    }

    /// <summary>DBImpl::WriteToWAL: the "DBImpl::WriteToWAL:log_entry" test hook, then merged_batch.VerifyChecksum().</summary>
    private static void VerifyWalRecordProtection(WriteBatch record)
    {
        TestHooks.InvokeSpan("DBImpl::WriteToWAL:log_entry", System.Runtime.InteropServices.MemoryMarshal.AsMemory(record.DataMemory).Span);
        record.VerifyChecksum();
    }
}
