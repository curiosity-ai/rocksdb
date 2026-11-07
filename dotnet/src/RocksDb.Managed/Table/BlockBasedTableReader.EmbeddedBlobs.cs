using System.Buffers;
using System.Runtime.CompilerServices;
using RocksDb.Managed.Db;
using RocksDb.Managed.Db.Blob;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Table;

/// <summary>
/// Embedded-blob SSTs (SstFileWriter::OpenWithEmbeddedBlobs, table/embedded_blob_sst.h): large values stored as
/// same-file SimpleGen2Blob records (payload, compression type byte, 4-byte checksum with the offset's context
/// modifier, db/blob/blob_gen2_format.cc) between the data blocks, referenced by kTypeBlobIndex entries whose blob
/// index has file number 0 (kCurrentFileBlobIndexFileNumber). A table carrying the "rocksdb.embedded.blob.stats"
/// property resolves those entries to plain values for point lookups and iterators
/// (BlockBasedTable::ResolveEmbeddedBlob, EmbeddedBlobResolvingIterator), so the rest of the read path, compaction
/// included, sees kTypeValue entries.
/// </summary>
/// <remarks>
/// Deviation: wide-column entities whose columns reference same-file blobs (version 2 entities written by native's
/// PutEntity into an embedded-blob SST) are not resolved; reading such a column fails like any blob column whose
/// blob file is unknown.
/// </remarks>
public sealed partial class BlockBasedTableReader
{
    /// <summary>BlockBasedTable::HasEmbeddedBlobRecords: the table has the embedded blob stats property.</summary>
    public bool HasEmbeddedBlobRecords => _hasEmbeddedBlobs;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsBlobIndexEntry(ReadOnlySpan<byte> internalKey) =>
        internalKey[^DbFormat.InternalKeyFooterSize] == (byte)EntryType.BlobIndex;

    /// <summary>BlobIndex::IsSameFile of an encoded blob index (false for malformed ones, left to the regular path).</summary>
    internal static bool IsSameFileBlobIndex(ReadOnlySpan<byte> blobIndex) =>
        BlobIndex.TryDecode(blobIndex, out BlobIndex index) is null && index.IsSameFile;

    /// <summary>
    /// Port of BlockBasedTable::ResolveEmbeddedBlob (+ ValidateEmbeddedBlobIndex, ReadAndVerifySimpleGen2BlobRecord):
    /// appends the payload of the same-file blob record <paramref name="blobIndex"/> references to
    /// <paramref name="output"/>. Incomplete with read_tier = kBlockCacheTier (a direct file read, no blob cache);
    /// Corruption for malformed indexes and records, or a checksum mismatch when verifying.
    /// </summary>
    internal void ResolveEmbeddedBlob(ReadOnlySpan<byte> blobIndex, IBufferWriter<byte> output, TableReadFlags flags)
    {
        string? error = BlobIndex.TryDecode(blobIndex, out BlobIndex index);
        if (error is not null) ThrowHelper.Corruption(error);
        if (!index.IsSameFile) ThrowHelper.InvalidArgument("Blob index does not reference this file");
        if (index.HasTtl) ThrowHelper.Corruption("Embedded blob index must not contain TTL metadata");
        if (!_hasEmbeddedBlobs) ThrowHelper.Corruption("Same-file blob index found without embedded blob records");
        ulong fileSize = (ulong)FileSize;
        if (index.Size > int.MaxValue - BlockBasedTableConstants.BlockTrailerSize) ThrowHelper.Corruption("Embedded blob record is too large");
        ulong recordSize = index.Size + BlockBasedTableConstants.BlockTrailerSize;
        if (index.Offset > fileSize || recordSize > fileSize - index.Offset) ThrowHelper.Corruption("Embedded blob record extends past file size");
        // Direct file read (no blob cache): a block-cache-only read cannot be served.
        if ((flags & TableReadFlags.BlockCacheTier) != 0)
        {
            throw new RocksDbException(StatusCode.Incomplete, "Result incomplete: Cannot read embedded blob in block-cache-only mode");
        }
        ReadDeadline.CheckBeforeIo();
        int payloadSize = (int)index.Size;
        int n = (int)recordSize;
        byte[] buf = RocksArrayPool<byte>.Shared.Rent(n);
        try
        {
            Span<byte> record = buf.AsSpan(0, n);
            int read = BlockFetcher.ReadFully(_file, (long)index.Offset, record);
            if (read != n)
            {
                ThrowHelper.Corruption("Incomplete blob record read at offset " + index.Offset + ": expected " + n + " bytes, got " + read);
            }
            var compression = (Compression.CompressionType)record[payloadSize];
            if (compression != index.Compression)
            {
                ThrowHelper.Corruption("Blob record compression mismatch at offset " + index.Offset + ": index expects " + (int)index.Compression
                    + ", record has " + (int)compression);
            }
            if (Verify(flags) && !BlockChecksum.Verify(_footer.ChecksumType, _footer.BaseContextChecksum, record, index.Offset))
            {
                ThrowHelper.Corruption("SimpleGen2 blob record checksum mismatch at offset " + index.Offset + " size " + payloadSize);
            }
            // SimpleGen2Blob payloads are currently written uncompressed only.
            if (compression != Compression.CompressionType.None)
            {
                ThrowHelper.Corruption("Blob record compression " + (int)compression + " is not supported at offset " + index.Offset);
            }
            output.Write(record[..payloadSize]);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(buf);
        }
    }

    /// <summary>
    /// The resolved form of a same-file blob index entry: <paramref name="internalKey"/> with type kTypeValue into
    /// <paramref name="key"/> (at least as long) and the payload into <paramref name="value"/>.
    /// </summary>
    internal void ResolveEmbeddedEntry(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> blobIndex, Span<byte> key, IBufferWriter<byte> value,
        TableReadFlags flags)
    {
        ResolveEmbeddedBlob(blobIndex, value, flags);
        internalKey.CopyTo(key);
        key[internalKey.Length - DbFormat.InternalKeyFooterSize] = (byte)EntryType.Value;
    }

    /// <summary>Calls <paramref name="visitor"/> with the resolved (kTypeValue) form of a same-file blob index entry.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool VisitEmbedded<TState>(ref TState state, TableEntryVisitor<TState> visitor, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> blobIndex,
        TableReadFlags flags) where TState : allows ref struct
    {
        if (!IsSameFileBlobIndex(blobIndex)) return visitor(ref state, internalKey, blobIndex);
        PooledBufferWriter w = ObjectPools.RentWriter();
        byte[] key = RocksArrayPool<byte>.Shared.Rent(internalKey.Length);
        try
        {
            ResolveEmbeddedEntry(internalKey, blobIndex, key, w, flags);
            return visitor(ref state, key.AsSpan(0, internalKey.Length), w.WrittenSpan);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(key);
            ObjectPools.ReturnWriter(w);
        }
    }

    /// <summary><see cref="VisitEmbedded{TState}"/> for a batched lookup.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool VisitEmbedded<TBatch>(TBatch batch, int k, ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> blobIndex, TableReadFlags flags)
        where TBatch : class, ITableMultiGetBatch
    {
        if (!IsSameFileBlobIndex(blobIndex)) return batch.Visit(k, internalKey, blobIndex);
        PooledBufferWriter w = ObjectPools.RentWriter();
        byte[] key = RocksArrayPool<byte>.Shared.Rent(internalKey.Length);
        try
        {
            ResolveEmbeddedEntry(internalKey, blobIndex, key, w, flags);
            return batch.Visit(k, key.AsSpan(0, internalKey.Length), w.WrittenSpan);
        }
        finally
        {
            RocksArrayPool<byte>.Shared.Return(key);
            ObjectPools.ReturnWriter(w);
        }
    }
}
