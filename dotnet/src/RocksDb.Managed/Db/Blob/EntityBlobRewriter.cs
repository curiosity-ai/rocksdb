using RocksDb.Managed.Db.Wide;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// Rewrites wide-column entities whose column values move to or from blob files: value separation of large columns
/// (CompactionIterator::ExtractLargeColumnValuesIfNeeded, also used by flush) and the blob GC of entity columns
/// (CompactionIterator::GarbageCollectEntityBlobsIfNeeded with FetchBlobsNeedingGC, RelocateBlobValues and
/// SerializeEntityAfterGC). Port of the entity parts of db/compaction/compaction_iterator.cc. Reusable scratch, not
/// thread-safe (one per flush / compaction job).
/// </summary>
internal sealed class EntityBlobRewriter : IDisposable
{
    private static readonly ReadOptions s_readOptions = new() { FillCache = false, VerifyChecksums = true };

    private readonly List<WideColumnSlot> _slots = new(8);
    private readonly EntityColumnList _columns = new();
    private readonly PooledBufferWriter _blobIndex = new(BlobIndex.MaxEncodedLength);
    private readonly PooledBufferWriter _blobValue = new(256);
    private readonly PooledBufferWriter _output = new(256);

    public long NumBlobsRead;
    public ulong TotalBlobBytesRead;
    public long NumBlobsRelocated;
    public ulong TotalBlobBytesRelocated;

    /// <summary>The entity written by the last successful rewrite.</summary>
    public ReadOnlySpan<byte> Output => _output.WrittenSpan;

    /// <summary>
    /// ExtractLargeColumnValuesIfNeeded: writes every non-empty column value of at least min_blob_size bytes to
    /// <paramref name="builder"/> and, if any moved, the version 2 entity to <see cref="Output"/> (returns true).
    /// Entities that already have blob columns are left alone (upstream's deliberate simplification: new large inline
    /// columns of such entities are extracted only once a GC rewrites them).
    /// </summary>
    public bool TryExtract(BlobFileBuilder builder, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> entity)
    {
        if (WideColumnSerialization.DeserializeWithBlobColumns(entity, _slots)) return false;
        return TryExtractDeserialized(builder, userKey, entity);
    }

    /// <summary><see cref="TryExtract"/> for an entity already decoded into <see cref="_slots"/> (no blob columns).</summary>
    private bool TryExtractDeserialized(BlobFileBuilder builder, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> entity)
    {
        _columns.Clear();
        bool any = false;
        foreach (WideColumnSlot s in _slots)
        {
            ReadOnlySpan<byte> name = entity.Slice(s.NameOffset, s.NameLength);
            ReadOnlySpan<byte> value = entity.Slice(s.ValueOffset, s.ValueLength);
            _blobIndex.Clear();
            if (builder.Add(userKey, value, _blobIndex))
            {
                _columns.Add(name, _blobIndex.WrittenSpan, isBlobIndex: true);
                any = true;
            }
            else
            {
                _columns.Add(name, value, isBlobIndex: false);
            }
        }
        if (!any) return false;
        _output.Clear();
        WideColumnSerialization.SerializeV2(_columns, _output);
        return true;
    }

    /// <summary>
    /// CompactionIterator::PrepareOutput for an entity: GC of the blob columns that reference files older than
    /// <paramref name="gcCutoffFileNumber"/> (0 = GC disabled) when the entity has blob columns, value separation with
    /// <paramref name="builder"/> otherwise. Returns true if <see cref="Output"/> holds a rewritten entity.
    /// </summary>
    public bool Prepare(BlobFileBuilder? builder, BlobSource source, ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> entity, ulong gcCutoffFileNumber)
    {
        if (!WideColumnSerialization.DeserializeWithBlobColumns(entity, _slots))
        {
            return builder is not null && TryExtractDeserialized(builder, userKey, entity);
        }
        if (gcCutoffFileNumber == 0) return false;
        // FetchBlobsNeedingGC: is there a blob column to relocate?
        bool needed = false;
        foreach (WideColumnSlot s in _slots)
        {
            if (!s.IsBlobIndex) continue;
            BlobIndex bi = BlobIndex.Decode(entity.Slice(s.ValueOffset, s.ValueLength));
            if (!bi.IsInlined && bi.FileNumber < gcCutoffFileNumber)
            {
                needed = true;
                break;
            }
        }
        if (!needed) return false;
        _columns.Clear();
        foreach (WideColumnSlot s in _slots)
        {
            ReadOnlySpan<byte> name = entity.Slice(s.NameOffset, s.NameLength);
            ReadOnlySpan<byte> value = entity.Slice(s.ValueOffset, s.ValueLength);
            if (!s.IsBlobIndex)
            {
                _columns.Add(name, value, isBlobIndex: false);
                continue;
            }
            BlobIndex bi = BlobIndex.Decode(value);
            if (bi.IsInlined || bi.FileNumber >= gcCutoffFileNumber)
            {
                _columns.Add(name, value, isBlobIndex: true); // not GC'd: keep the original reference
                continue;
            }
            _blobValue.Clear();
            ulong read = source.GetBlob(s_readOptions, userKey, bi.FileNumber, bi.Offset, bi.Size, bi.Compression, _blobValue);
            NumBlobsRead++;
            TotalBlobBytesRead += read;
            // RelocateBlobValues: written to a new blob file when the output gets blob files, inlined otherwise.
            _blobIndex.Clear();
            if (builder is not null && builder.Add(userKey, _blobValue.WrittenSpan, _blobIndex))
            {
                _columns.Add(name, _blobIndex.WrittenSpan, isBlobIndex: true);
                NumBlobsRelocated++;
                TotalBlobBytesRelocated += (ulong)_blobValue.WrittenCount;
            }
            else
            {
                _columns.Add(name, _blobValue.WrittenSpan, isBlobIndex: false);
            }
        }
        // SerializeEntityAfterGC: version 1 when no blob column is left.
        _output.Clear();
        WideColumnSerialization.Serialize(_columns, _output);
        return true;
    }

    public void Dispose()
    {
        _columns.Dispose();
        _blobIndex.Dispose();
        _blobValue.Dispose();
        _output.Dispose();
    }
}
