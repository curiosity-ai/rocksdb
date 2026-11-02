namespace RocksDb.Managed.Db;

/// <summary>
/// Callback interface for <see cref="WriteBatch.Iterate{THandler}(ReadOnlySpan{byte}, ref THandler)"/>. Port of
/// <c>WriteBatch::Handler</c> (include/rocksdb/write_batch.h).
/// <para>
/// Implement it on a <c>struct</c> and pass it by <c>ref</c>: the generic constraint makes the
/// calls direct (no boxing, no virtual dispatch). Spans are only valid during the callback. To fail iteration, throw
/// (e.g. a <see cref="RocksDbException"/>); C++ handlers return a Status instead. The C++ <c>TryAgain</c>
/// re-delivery protocol is not supported.
/// </para>
/// </summary>
public interface IWriteBatchHandler
{
    /// <summary>Called before each record; return false to stop iterating (the count is then not validated).</summary>
    bool Continue();
    void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    /// <summary>TimedPut (<c>kTypeValuePreferredSeqno</c>); <paramref name="value"/> excludes the packed write time.</summary>
    void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime);
    /// <summary>PutEntity; <paramref name="entity"/> is the serialized wide-column entity.</summary>
    void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity);
    void Delete(uint columnFamilyId, ReadOnlySpan<byte> key);
    void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key);
    void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey);
    void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex);
    /// <summary>A blob written with <c>PutLogData</c> (not applied to the DB).</summary>
    void LogData(ReadOnlySpan<byte> blob);
    void MarkBeginPrepare(bool unprepare);
    void MarkEndPrepare(ReadOnlySpan<byte> xid);
    /// <summary>A Noop marker; <paramref name="emptyBatch"/> is true if no records were seen since the last marker.</summary>
    void MarkNoop(bool emptyBatch);
    void MarkCommit(ReadOnlySpan<byte> xid);
    void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp);
    void MarkRollback(ReadOnlySpan<byte> xid);
}
