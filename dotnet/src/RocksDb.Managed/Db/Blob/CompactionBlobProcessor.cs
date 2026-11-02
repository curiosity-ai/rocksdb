using System.Buffers;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>
/// The blob work of a compaction iterator's PrepareOutput: extracting large values into blob files
/// (CompactionIterator::ExtractLargeValueIfNeeded) and relocating blobs of old blob files (integrated BlobDB garbage
/// collection, CompactionIterator::GarbageCollectBlobIfNeeded with a BlobFetcher over the input version, and
/// ComputeBlobGarbageCollectionCutoffFileNumber). Also resolves blob base values for merges (BlobFetcher). Port of the
/// blob parts of db/compaction/compaction_iterator.cc and db/blob/blob_fetcher.cc. Not thread-safe (one per job).
/// </summary>
internal sealed class CompactionBlobProcessor : IDisposable
{
    private static readonly ReadOptions s_readOptions = new() { FillCache = false, VerifyChecksums = true };

    private readonly BlobFileBuilder? _builder;
    private readonly BlobSource _source;
    private readonly VersionStorageInfo _inputVersion;
    private readonly ulong _gcCutoffFileNumber;
    private readonly PooledBufferWriter _blobIndex = new(BlobIndex.MaxEncodedLength);
    private readonly PooledBufferWriter _blobValue = new(256);
    private readonly PooledBufferWriter _filterValue = new(256);

    /// <param name="builder">Blob output (null when the output level does not get blob files).</param>
    /// <param name="source">The column family's blob source.</param>
    /// <param name="inputVersion">The compaction's input version (blob files that may be read).</param>
    /// <param name="enableGc">Whether blob garbage collection runs (enable_blob_garbage_collection or the manual policy).</param>
    /// <param name="ageCutoff">blob_garbage_collection_age_cutoff in [0, 1].</param>
    public CompactionBlobProcessor(BlobFileBuilder? builder, BlobSource source, VersionStorageInfo inputVersion, bool enableGc, double ageCutoff)
    {
        _builder = builder;
        _source = source;
        _inputVersion = inputVersion;
        _gcCutoffFileNumber = enableGc ? ComputeCutoffFileNumber(inputVersion, ageCutoff) : 0;
    }

    public BlobFileBuilder? Builder => _builder;
    public long NumBlobsRead;
    public ulong TotalBlobBytesRead;
    public long NumBlobsRelocated;
    public ulong TotalBlobBytesRelocated;

    /// <summary>The blob index written by the last successful extraction.</summary>
    public ReadOnlySpan<byte> BlobIndexOutput => _blobIndex.WrittenSpan;
    /// <summary>The blob value fetched by the last relocation.</summary>
    public ReadOnlySpan<byte> BlobValueOutput => _blobValue.WrittenSpan;

    /// <summary>
    /// ComputeBlobGarbageCollectionCutoffFileNumber: blobs in the oldest age_cutoff fraction of the blob files (by
    /// number) are relocated.
    /// </summary>
    public static ulong ComputeCutoffFileNumber(VersionStorageInfo version, double ageCutoff)
    {
        BlobFileMetaData[] blobFiles = version.BlobFiles;
        ulong cutoffIndex = (ulong)(ageCutoff * blobFiles.Length);
        if (cutoffIndex >= (ulong)blobFiles.Length) return ulong.MaxValue;
        return blobFiles[cutoffIndex].BlobFileNumber;
    }

    /// <summary>ExtractLargeValueIfNeededImpl: true if <paramref name="value"/> went to a blob file (index in <see cref="BlobIndexOutput"/>).</summary>
    public bool TryExtract(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value)
    {
        if (_builder is null) return false;
        _blobIndex.Clear();
        return _builder.Add(userKey, value, _blobIndex);
    }

    /// <summary>
    /// GarbageCollectBlobIfNeeded: returns false when the blob stays where it is. Otherwise the blob was read into
    /// <see cref="BlobValueOutput"/>; <paramref name="extracted"/> tells whether it was written to a new blob file
    /// (<see cref="BlobIndexOutput"/>) or must be stored inline.
    /// </summary>
    public bool TryRelocate(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndexValue, out bool extracted)
    {
        extracted = false;
        if (_gcCutoffFileNumber == 0) return false;
        BlobIndex index = BlobIndex.Decode(blobIndexValue);
        if (index.IsInlined || index.HasTtl) return false;
        if (index.FileNumber >= _gcCutoffFileNumber) return false;
        _blobValue.Clear();
        ulong read = _source.GetBlob(s_readOptions, userKey, index.FileNumber, index.Offset, index.Size, index.Compression, _blobValue);
        NumBlobsRead++;
        TotalBlobBytesRead += read;
        NumBlobsRelocated++;
        TotalBlobBytesRelocated += index.Size;
        extracted = TryExtract(userKey, _blobValue.WrittenSpan);
        return true;
    }

    /// <summary>Reads the blob of <paramref name="blobIndexValue"/> for the compaction filter (valid until the next call).</summary>
    public ReadOnlySpan<byte> FetchBlobForFilter(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndexValue)
    {
        _filterValue.Clear();
        FetchBlob(userKey, blobIndexValue, _filterValue);
        NumBlobsRead++;
        return _filterValue.WrittenSpan;
    }

    /// <summary>BlobFetcher::FetchBlob for a merge base value: appends the blob of <paramref name="blobIndexValue"/>.</summary>
    public void FetchBlob(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> blobIndexValue, IBufferWriter<byte> output) =>
        _source.GetBlob(_inputVersion, s_readOptions, userKey, blobIndexValue, output);

    public void Dispose()
    {
        _blobIndex.Dispose();
        _blobValue.Dispose();
        _filterValue.Dispose();
    }
}
