using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Rewrites a WriteBatch for blob direct write: Put values of at least min_blob_size bytes of column families with
/// enable_blob_direct_write are written to blob files and become PutBlobIndex records; PutEntity columns of that size
/// become blob-backed columns of a version 2 entity (one partition per entity); everything else is copied unchanged.
/// Port of rocksdb::BlobWriteBatchTransformer (db/blob/blob_write_batch_transformer.cc: PutCF, PutEntityCF,
/// MaybePreprocessWideColumns; TimedPut is passed through like upstream). Used by the write group leader only.
/// </summary>
internal sealed class BlobWriteBatchTransformer : IDisposable
{
    private readonly PooledBufferWriter _blobIndex = new(BlobIndex.MaxEncodedLength);
    private readonly PooledBufferWriter _entity = new(256);
    private readonly EntityColumnList _columns = new();
    private readonly List<WideColumnSlot> _slots = new(8);

    /// <summary>Managers that received blobs since the last <see cref="ClearTouched"/> (synced before a sync WAL write).</summary>
    public List<BlobDirectWriteManager> Touched { get; } = new();

    public void ClearTouched() => Touched.Clear();

    /// <summary>
    /// MaybeTransformBatchForBlobDirectWrite: returns a transformed copy of <paramref name="batch"/> (rented; the caller
    /// returns it) or null when nothing qualified. Batches with merge operands or transaction markers are not
    /// transformed (upstream skips batches with merges; transactional writes do not reach this path).
    /// </summary>
    public WriteBatch? Transform(WriteBatch batch, ColumnFamilyData?[] cfById)
    {
        if ((!batch.HasPut && !batch.HasPutEntity) || batch.HasMerge || batch.HasBeginPrepare || batch.HasEndPrepare || batch.HasCommit
            || batch.HasRollback)
        {
            return null;
        }
        WriteBatch output = WriteBatch.Rent();
        var handler = new Handler(this, output, cfById);
        try
        {
            WriteBatch.Iterate(batch.Data, ref handler);
        }
        catch
        {
            output.Dispose();
            throw;
        }
        if (!handler.Transformed)
        {
            output.Dispose();
            return null;
        }
        return output;
    }

    private static BlobDirectWriteManager? ManagerOf(ColumnFamilyData?[] cfById, uint cf, out ColumnFamilyData? cfd)
    {
        cfd = cf < (uint)cfById.Length ? cfById[cf] : null;
        BlobDirectWriteManager? m = cfd?.BlobDirectWrite;
        // Mutable options are read from the current options (blob_file_size, min_blob_size, blob_compression_type).
        if (m is not null) m.BlobFileSize = cfd!.Options.BlobFileSize;
        return m;
    }

    private void Touch(BlobDirectWriteManager m)
    {
        if (!Touched.Contains(m)) Touched.Add(m);
    }

    private struct Handler(BlobWriteBatchTransformer owner, WriteBatch output, ColumnFamilyData?[] cfById) : IWriteBatchHandler
    {
        public bool Transformed;

        public bool Continue() => true;

        public void Put(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            BlobDirectWriteManager? m = ManagerOf(cfById, columnFamilyId, out ColumnFamilyData? cfd);
            if (m is null || (ulong)value.Length < cfd!.Options.MinBlobSize)
            {
                output.Put(columnFamilyId, key, value);
                return;
            }
            PooledBufferWriter idx = owner._blobIndex;
            idx.Clear();
            m.WriteBlob(columnFamilyId, cfd.Options.BlobCompressionType, key, value, partition: -1, idx);
            owner.Touch(m);
            output.PutBlobIndex(columnFamilyId, key, idx.WrittenSpan);
            Transformed = true;
        }

        public void TimedPut(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ulong writeUnixTime) =>
            output.TimedPut(columnFamilyId, key, value, writeUnixTime);

        public void PutEntity(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> entity)
        {
            BlobDirectWriteManager? m = ManagerOf(cfById, columnFamilyId, out ColumnFamilyData? cfd);
            if (m is null)
            {
                output.PutEntitySerialized(columnFamilyId, key, entity);
                return;
            }
            List<WideColumnSlot> slots = owner._slots;
            if (WideColumnSerialization.DeserializeWithBlobColumns(entity, slots))
            {
                ThrowHelper.NotSupported("Blob direct write does not support pre-serialized wide entities with blob references");
            }
            ulong minBlobSize = cfd!.Options.MinBlobSize;
            EntityColumnList cols = owner._columns;
            cols.Clear();
            long partition = -1;
            foreach (WideColumnSlot s in slots)
            {
                ReadOnlySpan<byte> name = entity.Slice(s.NameOffset, s.NameLength);
                ReadOnlySpan<byte> value = entity.Slice(s.ValueOffset, s.ValueLength);
                if ((ulong)value.Length < minBlobSize)
                {
                    cols.Add(name, value, isBlobIndex: false);
                    continue;
                }
                if (partition < 0) partition = m.SelectEntityPartition(columnFamilyId, key, entity);
                PooledBufferWriter idx = owner._blobIndex;
                idx.Clear();
                m.WriteBlob(columnFamilyId, cfd.Options.BlobCompressionType, key, value, partition, idx);
                cols.Add(name, idx.WrittenSpan, isBlobIndex: true);
            }
            if (!cols.HasBlobColumns)
            {
                output.PutEntitySerialized(columnFamilyId, key, entity);
                return;
            }
            owner.Touch(m);
            PooledBufferWriter e = owner._entity;
            e.Clear();
            WideColumnSerialization.SerializeV2(cols, e);
            output.PutEntitySerialized(columnFamilyId, key, e.WrittenSpan);
            Transformed = true;
        }

        public void Delete(uint columnFamilyId, ReadOnlySpan<byte> key) => output.Delete(columnFamilyId, key);
        public void SingleDelete(uint columnFamilyId, ReadOnlySpan<byte> key) => output.SingleDelete(columnFamilyId, key);
        public void DeleteRange(uint columnFamilyId, ReadOnlySpan<byte> beginKey, ReadOnlySpan<byte> endKey) => output.DeleteRange(columnFamilyId, beginKey, endKey);
        public void Merge(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) => output.Merge(columnFamilyId, key, value);
        public void PutBlobIndex(uint columnFamilyId, ReadOnlySpan<byte> key, ReadOnlySpan<byte> blobIndex) => output.PutBlobIndex(columnFamilyId, key, blobIndex);
        public void LogData(ReadOnlySpan<byte> blob) => output.PutLogData(blob);
        public void MarkBeginPrepare(bool unprepare) => ThrowUnexpected();
        public void MarkEndPrepare(ReadOnlySpan<byte> xid) => ThrowUnexpected();
        public void MarkNoop(bool emptyBatch) => output.InsertNoop();
        public void MarkCommit(ReadOnlySpan<byte> xid) => ThrowUnexpected();
        public void MarkCommitWithTimestamp(ReadOnlySpan<byte> xid, ReadOnlySpan<byte> commitTimestamp) => ThrowUnexpected();
        public void MarkRollback(ReadOnlySpan<byte> xid) => ThrowUnexpected();

        private static void ThrowUnexpected() => throw new InvalidOperationException("Internal error: transaction markers in a blob direct write batch");
    }

    public void Dispose()
    {
        _blobIndex.Dispose();
        _entity.Dispose();
        _columns.Dispose();
    }
}
