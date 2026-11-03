using System.Buffers;
using RocksDb.Managed.Compression;
using RocksDb.Managed.Env;
using RocksDb.Managed.Util;

namespace RocksDb.Managed.Db.Blob;

/// <summary>BlobFileCreationReason (include/rocksdb/listener.h).</summary>
internal enum BlobFileCreationReason : byte
{
    Flush = 0,
    Compaction = 1,
    Recovery = 2,
}

/// <summary>
/// Writes values of at least <c>min_blob_size</c> bytes to blob files and returns blob indexes for them. Port of
/// rocksdb::BlobFileBuilder (db/blob/blob_file_builder.cc) and BlobLogWriter (db/blob/blob_log_writer.cc).
/// A blob file is opened lazily for the first blob, rolled over once it reaches <c>blob_file_size</c> and finished
/// (footer, fsync) by <see cref="Finish"/>. Every blob is compressed with <c>blob_compression_type</c>, even when that
/// makes it larger (LegacyForceBuiltinCompression). With a file_checksum_gen_factory the file checksum is computed
/// while writing and recorded as the addition's checksum method/value (empty without one, like upstream).
/// <para>Used by flush and compaction; one builder per job, not thread-safe. Per blob the builder does not allocate
/// (scratch buffers are reused).</para>
/// </summary>
internal sealed class BlobFileBuilder : IDisposable
{
    private readonly IFileSystem _fs;
    private readonly string _blobDir;
    private readonly Func<ulong> _newFileNumber;
    private readonly ulong _minBlobSize;
    private readonly ulong _blobFileSize;
    private readonly CompressionType _compression;
    private readonly bool _warmCache;
    private readonly BlobSource? _cacheTarget;
    private readonly uint _columnFamilyId;
    private readonly Statistics? _stats;
    private readonly WritableFileOptions _fileOptions;
    private readonly PooledBufferWriter _compressed = new(256);
    private readonly byte[] _header = new byte[Math.Max(BlobLogFormat.HeaderSize, BlobLogFormat.FooterSize)];
    private readonly byte[] _recordHeader = new byte[BlobLogFormat.RecordHeaderSize];

    private IWritableFile? _file;
    private ulong _fileNumber;
    private ulong _blobCount;
    private ulong _blobBytes;

    /// <param name="fs">File system.</param>
    /// <param name="blobDir">Directory of the blob files (cf_paths.front()).</param>
    /// <param name="newFileNumber">Allocates file numbers (registered as pending outputs by the caller).</param>
    /// <param name="options">Column family options (min_blob_size, blob_file_size, blob_compression_type, prepopulate_blob_cache).</param>
    /// <param name="columnFamilyId">Written into the blob file header.</param>
    /// <param name="reason">Why the blobs are written (prepopulate_blob_cache only warms the cache for flushes).</param>
    /// <param name="blobSource">The column family's blob source (blob cache), or null.</param>
    /// <param name="stats">Statistics or null.</param>
    /// <param name="fileOptions">Write buffering options of the blob files.</param>
    public BlobFileBuilder(IFileSystem fs, string blobDir, Func<ulong> newFileNumber, ColumnFamilyOptions options, uint columnFamilyId,
        BlobFileCreationReason reason, BlobSource? blobSource, Statistics? stats, WritableFileOptions fileOptions)
    {
        _fs = fs;
        _blobDir = blobDir;
        _newFileNumber = newFileNumber;
        _minBlobSize = options.MinBlobSize;
        _blobFileSize = options.BlobFileSize;
        _compression = options.BlobCompressionType;
        if (_compression != CompressionType.None && !BlockCompression.IsSupported(_compression))
        {
            ThrowHelper.NotSupported("Blob compression type " + _compression.ToString() + " is not supported");
        }
        _warmCache = options.PrepopulateBlobCache == PrepopulateBlobCache.FlushOnly && reason == BlobFileCreationReason.Flush;
        _cacheTarget = blobSource;
        _columnFamilyId = columnFamilyId;
        _stats = stats;
        _fileOptions = fileOptions;
    }

    /// <summary>Blob files written successfully (finished), in creation order.</summary>
    public List<BlobFileAddition> Additions { get; } = new();

    /// <summary>
    /// Set by the job right before the version edit with <see cref="Additions"/> is written to the MANIFEST: from then on
    /// the edit may be durable even if the write reports a failure, so the files must not be deleted as failed outputs
    /// (unreferenced ones are removed by the obsolete-file scan at the next open).
    /// </summary>
    public bool Committed { get; set; }

    /// <summary>Numbers of all blob files created (also unfinished ones, for cleanup on failure).</summary>
    public List<ulong> CreatedFileNumbers { get; } = new();

    /// <summary>Path of blob file #<paramref name="number"/>.</summary>
    public string BlobFilePath(ulong number) => FileNames.BlobFileName(_blobDir, number);

    /// <summary>
    /// BlobFileBuilder::Add: if <paramref name="value"/> qualifies (non-empty and at least min_blob_size bytes), writes it
    /// to the current blob file, appends its blob index to <paramref name="blobIndex"/> and returns true; otherwise
    /// returns false and writes nothing.
    /// </summary>
    public bool Add(ReadOnlySpan<byte> userKey, ReadOnlySpan<byte> value, IBufferWriter<byte> blobIndex)
    {
        if (value.IsEmpty || (ulong)value.Length < _minBlobSize) return false;
        OpenBlobFileIfNeeded();
        ReadOnlySpan<byte> blob = value;
        if (_compression != CompressionType.None)
        {
            _compressed.Clear();
            BlockCompression.CompressForced(_compression, value, BlockCompression.DefaultCompressionLevel, _compressed);
            blob = _compressed.WrittenSpan;
        }
        ulong fileNumber = _fileNumber;
        ulong blobOffset = WriteBlobToFile(userKey, blob);
        CloseBlobFileIfNeeded();
        if (_warmCache) _cacheTarget?.InsertIntoCache(fileNumber, blobOffset, value);
        BlobIndex.EncodeBlob(blobIndex, fileNumber, blobOffset, (ulong)blob.Length, _compression);
        return true;
    }

    /// <summary>BlobFileBuilder::Finish: closes the open blob file, if any.</summary>
    public void Finish()
    {
        if (_file is not null) CloseBlobFile();
    }

    /// <summary>BlobFileBuilder::Abandon: drops the open blob file without a footer (the caller deletes the files).</summary>
    public void Abandon()
    {
        if (_file is null) return;
        try { _file.Dispose(); }
        catch (Exception e) when (e is IOException or RocksDbException) { }
        _file = null;
        _blobCount = 0;
        _blobBytes = 0;
    }

    /// <summary>Deletes every blob file this builder created (after a failed job).</summary>
    public void DeleteCreatedFiles()
    {
        Abandon();
        foreach (ulong n in CreatedFileNumbers)
        {
            try { _fs.DeleteFile(BlobFilePath(n)); }
            catch (Exception e) when (e is IOException or RocksDbException or UnauthorizedAccessException) { }
        }
    }

    private void OpenBlobFileIfNeeded()
    {
        if (_file is not null) return;
        ulong number = _newFileNumber();
        string path = BlobFilePath(number);
        // Recorded right after the open so a failed job can clean it up (output_file_paths_).
        CreatedFileNumbers.Add(number);
        _file = FileChecksumHelper.CreateWritable(_fs, path, _fileOptions);
        _fileNumber = number;
        _blobCount = 0;
        _blobBytes = 0;
        // BlobLogHeader(column_family_id, blob_compression_type, has_ttl = false, expiration_range = (0, 0)).
        BlobLogFormat.EncodeHeader(_header, _columnFamilyId, _compression, hasTtl: false, 0, 0);
        _file.Append(_header.AsSpan(0, BlobLogFormat.HeaderSize));
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, BlobLogFormat.HeaderSize);
    }

    /// <summary>BlobLogWriter::AddRecord + EmitPhysicalRecord; returns the offset of the blob value.</summary>
    private ulong WriteBlobToFile(ReadOnlySpan<byte> key, ReadOnlySpan<byte> blob)
    {
        IWritableFile f = _file!;
        ulong recordOffset = (ulong)f.Size;
        BlobLogFormat.EncodeRecordHeader(_recordHeader, key, blob, expiration: 0);
        f.Append(_recordHeader);
        f.Append(key);
        f.Append(blob);
        _blobCount++;
        ulong recordBytes = BlobLogFormat.RecordHeaderSize + (ulong)key.Length + (ulong)blob.Length;
        _blobBytes += recordBytes;
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, recordBytes);
        return recordOffset + BlobLogFormat.RecordHeaderSize + (ulong)key.Length;
    }

    private void CloseBlobFileIfNeeded()
    {
        if ((ulong)_file!.Size < _blobFileSize) return;
        CloseBlobFile();
    }

    /// <summary>BlobFileBuilder::CloseBlobFile / BlobLogWriter::AppendFooter: footer, fsync, close, record the addition.</summary>
    private void CloseBlobFile()
    {
        IWritableFile f = _file!;
        BlobLogFormat.EncodeFooter(_header, _blobCount, 0, 0);
        f.Append(_header.AsSpan(0, BlobLogFormat.FooterSize));
        f.Sync();
        // BlobLogWriter::AppendFooter: checksum method/value from the writer (empty without a checksum factory).
        (byte[] checksum, string method) = FileChecksumHelper.GetChecksum(f);
        if (checksum.Length == 0) method = "";
        f.Dispose();
        _file = null;
        _stats?.RecordTick(Ticker.BlobDbBlobFileBytesWritten, BlobLogFormat.FooterSize);
        _stats?.RecordTick(Ticker.BlobDbBlobFileSynced);
        Additions.Add(new BlobFileAddition(_fileNumber, _blobCount, _blobBytes, method, checksum));
        _blobCount = 0;
        _blobBytes = 0;
    }

    public void Dispose()
    {
        Abandon();
        _compressed.Dispose();
    }
}
